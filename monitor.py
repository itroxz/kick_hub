#!/usr/bin/env python3
"""
Monitor resiliente de espectadores ao vivo na Kick, com detecção de sessões.

Funcionalidades novas nesta versão:
- Detecção de sessões (livestreams) por canal: cria sessão quando live começa e encerra quando termina.
- Associa cada amostra (`samples`) a `session_id` quando aplicável.
- Armazena métricas de sessão (avg_viewers, max_viewers, sample_count, start/end ts).

Use `run_supervisor.py` para reiniciar o processo se ele encerrar.
"""
import urllib.request
import urllib.error
import json
import sqlite3
import threading
import time
import os
import sys
import logging
from datetime import datetime, timezone

# Allow overriding DB paths via environment (useful in containers)
DB_PATH = os.environ.get('MONITOR_DB_PATH') or os.path.join(os.path.dirname(__file__), "kick_monitor.sqlite3")
# channels.txt fallback path
CHANNELS_FILE = os.environ.get('CHANNELS_FILE') or os.path.join(os.path.dirname(__file__), "channels.txt")
# fallback path for the web/dashboard DB used to store channels
FDS_DB_FALLBACK = os.environ.get('FDS_DB_PATH') or os.path.join(os.path.dirname(__file__), "fds_bot.db")
POLL_INTERVAL = 30  # segundos
SUPERVISOR_INTERVAL = 5  # segundos, checa status dos workers
RECONCILE_INTERVAL = 60  # segundos entre runs do reconciler
STALE_MINUTES = 10  # minutos de inatividade para considerar uma session encerrada

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def init_db(path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # Create base tables if they don't exist
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            ts INTEGER NOT NULL,
            viewers INTEGER,
            is_live INTEGER,
            raw_json TEXT,
            session_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS peaks (
            channel TEXT PRIMARY KEY,
            peak_overall INTEGER DEFAULT 0,
            peak_overall_ts INTEGER,
            peak_daily INTEGER DEFAULT 0,
            peak_daily_date TEXT,
            peak_weekly INTEGER DEFAULT 0,
            peak_week_start TEXT,
            peak_monthly INTEGER DEFAULT 0,
            peak_month TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            livestream_id TEXT,
            title TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            avg_viewers REAL,
            max_viewers INTEGER,
            sample_count INTEGER
        )
        """
    )
    # channels table (for DB-based channel management)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """
    )
    conn.commit()

    # Automatic migrations: ensure expected columns exist; add them when missing.
    def ensure_columns(table, expected):
        try:
            cur.execute(f"PRAGMA table_info({table})")
            existing = [r[1] for r in cur.fetchall()]
            for col, col_def in expected.items():
                if col not in existing:
                    logging.info("Migrating DB: adding column %s to %s", col, table)
                    try:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
                    except Exception:
                        logging.exception("Falha ao adicionar coluna %s.%s", table, col)
        except Exception:
            logging.exception("Erro ao verificar colunas para tabela %s", table)

    samples_expected = {
        'raw_json': 'TEXT',
        'session_id': 'INTEGER',
    }
    peaks_expected = {
        'peak_overall_ts': 'INTEGER',
        'peak_daily_date': 'TEXT',
        'peak_week_start': 'TEXT',
        'peak_month': 'TEXT',
    }
    sessions_expected = {
        'livestream_id': 'TEXT',
        'title': 'TEXT',
        'start_ts': 'INTEGER',
        'end_ts': 'INTEGER',
        'avg_viewers': 'REAL',
        'max_viewers': 'INTEGER',
        'sample_count': 'INTEGER',
    }

    ensure_columns('samples', samples_expected)
    ensure_columns('peaks', peaks_expected)
    ensure_columns('sessions', sessions_expected)

    # Helpful indexes
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_samples_channel_ts ON samples(channel, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_channel_start ON sessions(channel, start_ts)")
    except Exception:
        logging.exception("Falha ao criar índices")

    conn.commit()
    conn.close()


def read_channels(path=CHANNELS_FILE):
    # Try reading channels from kick_monitor.sqlite3 (channels table)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name FROM channels ORDER BY name")
        rows = cur.fetchall()
        conn.close()
        if rows:
            logging.info("Loaded %s channels from %s table", len(rows), DB_PATH)
            return [r[0] for r in rows]
    except Exception:
        logging.debug("No channels table in monitor DB or failed to read")

    # Fallback: try fds_bot.db (shared DB used by the web dashboard)
    try:
        other_db = os.path.join(os.path.dirname(__file__), "fds_bot.db")
        if os.path.exists(other_db):
            conn = sqlite3.connect(other_db)
            cur = conn.cursor()
            cur.execute("SELECT name FROM channels ORDER BY name")
            rows = cur.fetchall()
            conn.close()
            if rows:
                logging.info("Loaded %s channels from %s", len(rows), other_db)
                return [r[0] for r in rows]
    except Exception:
        logging.debug("Failed to read channels from fds_bot.db")

    # Final fallback: channels.txt
    if not os.path.exists(path):
        logging.warning("Arquivo de canais não encontrado: %s", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines()]
    channels = [l for l in lines if l and not l.startswith("#")]
    return channels


def fetch_channel(channel):
    url = f"https://kick.com/api/v1/channels/{channel}"
    req = urllib.request.Request(url, headers={"User-Agent": "kick-monitor/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = resp.read().decode("utf-8")
            j = json.loads(data)
            viewers = None
            is_live = 0
            if isinstance(j, dict):
                livestream = j.get("livestream") or j.get("live_stream")
                if isinstance(livestream, dict):
                    viewers = livestream.get("viewer_count") or livestream.get("viewers")
                    is_live = 1 if livestream.get("is_live") else 0
                if viewers is None:
                    viewers = j.get("viewers") or j.get("viewer_count")
            if viewers is None:
                viewers = -1
            return int(viewers), int(is_live), j
    except urllib.error.HTTPError as e:
        logging.error("HTTP error ao buscar %s: %s", channel, e)
        return -1, 0, {"error": str(e)}
    except Exception as e:
        logging.error("Erro ao buscar %s: %s", channel, e)
        return -1, 0, {"error": str(e)}


def save_sample(channel, viewers, is_live, raw_json, session_id=None, path=DB_PATH):
    ts = int(time.time())
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # serializar JSON de forma segura
    try:
        raw_str = json.dumps(raw_json, ensure_ascii=False, default=str)
    except Exception:
        try:
            raw_str = str(raw_json)
        except Exception:
            raw_str = None
    try:
        cur.execute(
            "INSERT INTO samples (channel, ts, viewers, is_live, raw_json, session_id) VALUES (?, ?, ?, ?, ?, ?)",
            (channel, ts, viewers, is_live, raw_str, session_id),
        )
    except Exception:
        logging.exception("DB insert falhou para sample (tentando fallback sem raw_json)")
        try:
            cur.execute(
                "INSERT INTO samples (channel, ts, viewers, is_live, session_id) VALUES (?, ?, ?, ?, ?)",
                (channel, ts, viewers, is_live, session_id),
            )
        except Exception:
            logging.exception("DB insert falhou no fallback para sample; descartando amostra")
            conn.rollback()
            conn.close()
            return
    conn.commit()
    conn.close()
    try:
        update_peaks(channel, ts, viewers, path)
    except Exception as e:
        logging.exception("Falha ao atualizar picos: %s", e)


def iso_date(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def iso_month(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def week_start_iso(ts):
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    # ISO week start (Monday)
    start = d.replace(hour=0, minute=0, second=0, microsecond=0)
    start -= timedelta(days=(start.weekday()))
    return start.strftime("%Y-%m-%d")


from datetime import timedelta


def update_peaks(channel, ts, viewers, path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT peak_overall, peak_daily, peak_daily_date, peak_weekly, peak_week_start, peak_monthly, peak_month FROM peaks WHERE channel = ?", (channel,))
    row = cur.fetchone()
    today = iso_date(ts)
    month = iso_month(ts)
    # compute week start iso (YYYY-MM-DD)
    week_start = week_start_iso(ts)

    if row is None:
        # insert new
        cur.execute(
            "INSERT INTO peaks (channel, peak_overall, peak_overall_ts, peak_daily, peak_daily_date, peak_weekly, peak_week_start, peak_monthly, peak_month) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (channel, viewers, ts, viewers, today, viewers, week_start, viewers, month),
        )
    else:
        (peak_overall, peak_daily, peak_daily_date, peak_weekly, peak_week_start, peak_monthly, peak_month) = row
        # overall
        if viewers > (peak_overall or 0):
            cur.execute("UPDATE peaks SET peak_overall = ?, peak_overall_ts = ? WHERE channel = ?", (viewers, ts, channel))
        # daily
        if peak_daily_date != today:
            # new day -> reset daily peak
            cur.execute("UPDATE peaks SET peak_daily = ?, peak_daily_date = ? WHERE channel = ?", (viewers, today, channel))
        else:
            if viewers > (peak_daily or 0):
                cur.execute("UPDATE peaks SET peak_daily = ? WHERE channel = ?", (viewers, channel))
        # weekly (compare week_start)
        if peak_week_start != week_start:
            cur.execute("UPDATE peaks SET peak_weekly = ?, peak_week_start = ? WHERE channel = ?", (viewers, week_start, channel))
        else:
            if viewers > (peak_weekly or 0):
                cur.execute("UPDATE peaks SET peak_weekly = ? WHERE channel = ?", (viewers, channel))
        # monthly
        if peak_month != month:
            cur.execute("UPDATE peaks SET peak_monthly = ?, peak_month = ? WHERE channel = ?", (viewers, month, channel))
        else:
            if viewers > (peak_monthly or 0):
                cur.execute("UPDATE peaks SET peak_monthly = ? WHERE channel = ?", (viewers, channel))
    conn.commit()
    conn.close()


def _get_open_session(channel, path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT id, livestream_id FROM sessions WHERE channel = ? AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1", (channel,))
    r = cur.fetchone()
    conn.close()
    if r:
        return {'id': r[0], 'livestream_id': r[1]}
    return None


def _create_session(channel, livestream_id, title, start_ts, path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("INSERT INTO sessions (channel, livestream_id, title, start_ts) VALUES (?, ?, ?, ?)", (channel, livestream_id, title, start_ts))
    sid = cur.lastrowid
    conn.commit()
    conn.close()
    logging.info("Nova session criada para %s: id=%s livestream_id=%s", channel, sid, livestream_id)
    return sid


def _close_session(session_id, end_ts, path=DB_PATH):
    # atualiza end_ts e calcula métricas a partir de samples
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("UPDATE sessions SET end_ts = ? WHERE id = ?", (end_ts, session_id))
    # compute metrics
    cur.execute("SELECT AVG(viewers), MAX(viewers), COUNT(*) FROM samples WHERE session_id = ?", (session_id,))
    avg_v, max_v, cnt = cur.fetchone()
    cur.execute("UPDATE sessions SET avg_viewers = ?, max_viewers = ?, sample_count = ? WHERE id = ?", (avg_v or 0, max_v or 0, cnt or 0, session_id))
    conn.commit()
    conn.close()
    logging.info("Session %s fechada: end_ts=%s avg=%.2f max=%s samples=%s", session_id, end_ts, avg_v or 0, max_v or 0, cnt or 0)


def worker_main_loop(channel, stop_event):
    logging.info("Worker iniciado para: %s", channel)
    # recuperar sessão aberta se existir
    current = _get_open_session(channel)
    while not stop_event.is_set():
        try:
            viewers, is_live, raw = fetch_channel(channel)
            # extrair id da livestream se disponível
            livestream = None
            if isinstance(raw, dict):
                livestream = raw.get('livestream') or raw.get('live_stream')
            ls_id = None
            title = None
            if isinstance(livestream, dict):
                ls_id = str(livestream.get('id') or livestream.get('uuid') or '')
                title = livestream.get('session_title') or livestream.get('title') or None

            ts = int(time.time())
            if is_live and ls_id:
                # se não houver session atual ou livestream mudou, criar nova session
                if not current or str(current.get('livestream_id') or '') != ls_id:
                    sid = _create_session(channel, ls_id, title, ts)
                    current = {'id': sid, 'livestream_id': ls_id}
                # salvar sample com session_id
                save_sample(channel, viewers, is_live, raw, session_id=current['id'])
            else:
                # não está ao vivo
                save_sample(channel, viewers, is_live, raw, session_id=None)
                if current:
                    # fechar session
                    _close_session(current['id'], ts)
                    current = None
            logging.info("%s -> viewers=%s is_live=%s session=%s", channel, viewers, is_live, current['id'] if current else None)
        except Exception:
            logging.exception("Erro não tratado no worker para %s", channel)
            # se ocorrer um erro grave, o loop continua e tentará novamente
        # espera com interrupção responsiva
        for _ in range(POLL_INTERVAL):
            if stop_event.is_set():
                break
            time.sleep(1)
    # ao parar, fechar sessão aberta se houver
    if current:
        _close_session(current['id'], int(time.time()))
    logging.info("Worker parado para: %s", channel)


class Supervisor:
    def __init__(self, channels):
        self.channels = channels
        self.stop_event = threading.Event()
        self.threads = {}
        self._reconciler_thread = None

    def start(self):
        for ch in self.channels:
            self._start_worker(ch)
        # start reconciler
        self._reconciler_thread = threading.Thread(target=self._reconciler_loop, daemon=True)
        self._reconciler_thread.start()
        # loop supervisor
        try:
            while not self.stop_event.is_set():
                time.sleep(SUPERVISOR_INTERVAL)
                # check threads
                for ch in list(self.channels):
                    t = self.threads.get(ch)
                    if t is None or not t.is_alive():
                        logging.warning("Worker para %s morto. Reiniciando...", ch)
                        self._start_worker(ch)
        except KeyboardInterrupt:
            logging.info("Supervisor recebendo KeyboardInterrupt, parando...")
            self.stop()

    def _start_worker(self, ch):
        # ensure previous thread stop
        stop_ev = threading.Event()
        t = threading.Thread(target=worker_main_loop, args=(ch, stop_ev), daemon=True)
        t._stop_event = stop_ev
        t.start()
        self.threads[ch] = t

    def stop(self):
        self.stop_event.set()
        # signal workers to stop
        for ch, t in self.threads.items():
            ev = getattr(t, "_stop_event", None)
            if ev:
                ev.set()
        # join
        for ch, t in self.threads.items():
            t.join(timeout=5)
        # stop reconciler
        if self._reconciler_thread:
            self._reconciler_thread.join(timeout=2)

    def _reconciler_loop(self):
        logging.info("Reconciler started: closing stale sessions older than %s minutes", STALE_MINUTES)
        while not self.stop_event.is_set():
            try:
                # close stale sessions as before
                reconcile_sessions()
                # reload channels from DB and reconcile workers
                try:
                    db_channels = list(read_channels())
                    db_set = set(db_channels)
                    current_set = set(self.channels)
                    # start workers for newly added channels
                    for ch in sorted(db_set - current_set):
                        logging.info("Reconciler: new channel detected %s, starting worker", ch)
                        self.channels.append(ch)
                        self._start_worker(ch)
                    # stop workers for removed channels
                    for ch in sorted(current_set - db_set):
                        logging.info("Reconciler: channel removed %s, stopping worker", ch)
                        t = self.threads.get(ch)
                        if t:
                            ev = getattr(t, "_stop_event", None)
                            if ev:
                                ev.set()
                            try:
                                t.join(timeout=5)
                            except Exception:
                                logging.exception("Erro ao juntar thread de %s", ch)
                            # clean up
                            try:
                                del self.threads[ch]
                            except KeyError:
                                pass
                        try:
                            self.channels.remove(ch)
                        except ValueError:
                            pass
                except Exception:
                    logging.exception("Erro ao reconciliar lista de canais")
            except Exception:
                logging.exception("Erro no reconciler")
            for _ in range(RECONCILE_INTERVAL):
                if self.stop_event.is_set():
                    break
                time.sleep(1)


def one_shot(channels):
    init_db()
    for ch in channels:
        viewers, is_live, raw = fetch_channel(ch)
        save_sample(ch, viewers, is_live, raw)
        print(f"{ch}: viewers={viewers} is_live={is_live}")


def reconcile_sessions(path=DB_PATH):
    """Fechar sessions que estão abertas mas não receberam samples nos últimos STALE_MINUTES."""
    cutoff = int(time.time()) - STALE_MINUTES * 60
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # encontrar sessions abertas
    cur.execute("SELECT id, channel, start_ts FROM sessions WHERE end_ts IS NULL")
    open_sessions = cur.fetchall()
    for sid, channel, start_ts in open_sessions:
        # verificar último sample para esta session
        cur.execute("SELECT MAX(ts) FROM samples WHERE session_id = ?", (sid,))
        r = cur.fetchone()
        last_ts = r[0] if r else None
        if last_ts is None:
            # nenhum sample; se start_ts antigo, fechar
            if start_ts and start_ts < cutoff:
                logging.info("Reconciling: fechando session %s (nenhum sample) para %s", sid, channel)
                _close_session(sid, int(time.time()), path)
        else:
            if last_ts < cutoff:
                logging.info("Reconciling: fechando session %s (ultimo sample %s) para %s", sid, last_ts, channel)
                _close_session(sid, int(time.time()), path)
    conn.close()


def main(argv):
    once = False
    if len(argv) > 1 and argv[1] in ("--once", "-1"):
        once = True

    channels = read_channels()
    if not channels:
        print("Nenhum canal encontrado em channels.txt. Por favor, adicione slugs de canais (ex: xqc) em uma linha por canal.")
        sys.exit(1)

    init_db()

    if once:
        one_shot(channels)
        return

    sup = Supervisor(channels)
    sup.start()


if __name__ == "__main__":
    main(sys.argv)
