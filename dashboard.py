import os
import sqlite3
from flask import Flask, render_template_string, request

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "fds_bot.db")


def init_channels_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def get_channels_list():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM channels ORDER BY name")
    channels = [row[0] for row in c.fetchall()]
    conn.close()
    return channels


def add_channel(name):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO channels (name) VALUES (?)", (name,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def edit_channel(old_name, new_name):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("UPDATE channels SET name=? WHERE name=?", (new_name, old_name))
        conn.commit()
        return c.rowcount > 0
    finally:
        conn.close()


def delete_channel(name):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM channels WHERE name=?", (name,))
    conn.commit()
    deleted = c.rowcount > 0
    conn.close()
    return deleted


init_channels_table()


@app.route('/')
def index():
    channels = get_channels_list()
    return render_template_string('''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <title>Dashboard</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <h1>Dashboard</h1>
    <p><a class="btn btn-primary" href="/usuarios">Gerenciar canais</a></p>
    <h5>Canais (SQLite)</h5>
    <ul>
    {% for ch in channels %}
      <li>{{ch}}</li>
    {% endfor %}
    </ul>
  </div>
</body>
</html>
''', channels=channels)


@app.route('/usuarios', methods=['GET', 'POST'])
def usuarios():
    msg = None
    channels = get_channels_list()
    if request.method == 'POST':
        action = request.form.get('action')
        name = request.form.get('name', '').strip()
        old_name = request.form.get('old_name', '').strip()
        if action == 'add' and name:
            if add_channel(name):
                msg = f'Canal {name} adicionado.'
            else:
                msg = 'Canal já existe.'
        elif action == 'edit' and old_name and name:
            if edit_channel(old_name, name):
                msg = f'Canal {old_name} alterado para {name}.'
            else:
                msg = 'Canal não encontrado ou nome já existe.'
        elif action == 'delete' and old_name:
            if delete_channel(old_name):
                msg = f'Canal {old_name} removido.'
            else:
                msg = 'Canal não encontrado.'
        channels = get_channels_list()

    return render_template_string('''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <title>Gerenciar Canais</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <h2 class="mb-4">Gerenciar Canais</h2>
    {% if msg %}<div class="alert alert-info">{{msg}}</div>{% endif %}
    <form method="post" class="mb-3">
      <div class="input-group mb-2">
        <input type="text" name="name" class="form-control" placeholder="Novo canal" required>
        <button class="btn btn-success" name="action" value="add" type="submit">Adicionar</button>
      </div>
    </form>
    <table class="table table-bordered">
      <thead><tr><th>Canal</th><th>Ações</th></tr></thead>
      <tbody>
      {% for ch in channels %}
        <tr>
          <form method="post">
            <td>
              <input type="hidden" name="old_name" value="{{ch}}">
              <input type="text" name="name" value="{{ch}}" class="form-control">
            </td>
            <td>
              <button class="btn btn-primary btn-sm" name="action" value="edit" type="submit">Editar</button>
              <button class="btn btn-danger btn-sm" name="action" value="delete" type="submit" onclick="return confirm('Remover canal?')">Remover</button>
            </td>
          </form>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    <a href="/" class="btn btn-outline-secondary">Voltar</a>
  </div>
</body>
</html>
''', channels=channels, msg=msg)


if __name__ == '__main__':
    app.run(debug=True)
    <html>
    <head>
        channels = get_channels_list()
        if request.method == 'POST':
          action = request.form.get('action')
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
      <title>Gerenciar Canais</title>
      <style>body{padding:20px}</style>
    </head>
    <body>
      <div class="container">
        <h2 class="mb-4">Gerenciar Canais</h2>
        {% if msg %}<div class="alert alert-info">{{msg}}</div>{% endif %}
        <form method="post" class="mb-3">
          <div class="input-group mb-2">
            <input type="text" name="name" class="form-control" placeholder="Novo canal" required>
            <button class="btn btn-success" name="action" value="add" type="submit">Adicionar</button>
          </div>
        </form>
        <table class="table table-bordered">
          <thead><tr><th>Canal</th><th>Ações</th></tr></thead>
          <tbody>
          {% for ch in channels %}
            <tr>
              <form method="post">
                <td>
                  <input type="hidden" name="old_name" value="{{ch}}">
                  <input type="text" name="name" value="{{ch}}" class="form-control">
                </td>
                <td>
                  <button class="btn btn-primary btn-sm" name="action" value="edit" type="submit">Editar</button>
                  <button class="btn btn-danger btn-sm" name="action" value="delete" type="submit" onclick="return confirm('Remover canal?')">Remover</button>
                </td>
              </form>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <a href="/" class="btn btn-outline-secondary">Voltar</a>
      </div>
    </body>
    </html>
    ''', channels=channels, msg=msg)
from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta

app = Flask(__name__)
CHANNELS_FILE = os.path.join(os.path.dirname(__file__), "channels.txt")

def get_channels_list():
    if not os.path.exists(CHANNELS_FILE):
        return []
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def save_channels_list(channels):
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        for ch in channels:
            f.write(ch + "\n")

@app.route('/usuarios', methods=['GET', 'POST'])
def usuarios():
    msg = None
    channels = get_channels_list()
    if request.method == 'POST':
        action = request.form.get('action')
        name = request.form.get('name', '').strip()
        old_name = request.form.get('old_name', '').strip()
        if action == 'add' and name:
            if name not in channels:
                channels.append(name)
                save_channels_list(channels)
                msg = f'Canal {name} adicionado.'
            else:
                msg = 'Canal já existe.'
        elif action == 'edit' and old_name and name:
            if old_name in channels:
                idx = channels.index(old_name)
                channels[idx] = name
                save_channels_list(channels)
                msg = f'Canal {old_name} alterado para {name}.'
            else:
                msg = 'Canal não encontrado.'
        elif action == 'delete' and old_name:
            if old_name in channels:
                channels.remove(old_name)
                save_channels_list(channels)
                msg = f'Canal {old_name} removido.'
            else:
                msg = 'Canal não encontrado.'
    return render_template_string('''
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
      <title>Gerenciar Canais</title>
      <style>body{padding:20px}</style>
    </head>
    <body>
      <div class="container">
        <h2 class="mb-4">Gerenciar Canais</h2>
        {% if msg %}<div class="alert alert-info">{{msg}}</div>{% endif %}
        <form method="post" class="mb-3">
          <div class="input-group mb-2">
            <input type="text" name="name" class="form-control" placeholder="Novo canal" required>
            <button class="btn btn-success" name="action" value="add" type="submit">Adicionar</button>
          </div>
        </form>
        <table class="table table-bordered">
          <thead><tr><th>Canal</th><th>Ações</th></tr></thead>
          <tbody>
          {% for ch in channels %}
            <tr>
              <form method="post">
                <td>
                  <input type="hidden" name="old_name" value="{{ch}}">
                  <input type="text" name="name" value="{{ch}}" class="form-control">
                </td>
                <td>
                  <button class="btn btn-primary btn-sm" name="action" value="edit" type="submit">Editar</button>
                  <button class="btn btn-danger btn-sm" name="action" value="delete" type="submit" onclick="return confirm('Remover canal?')">Remover</button>
                </td>
              </form>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <a href="/" class="btn btn-outline-secondary">Voltar</a>
      </div>
    </body>
    </html>
    ''', channels=channels, msg=msg)
from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta
app = Flask(__name__)
app = Flask(__name__)
from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta
CHANNELS_FILE = os.path.join(os.path.dirname(__file__), "channels.txt")

def get_channels_list():
    if not os.path.exists(CHANNELS_FILE):
        return []
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def save_channels_list(channels):
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        for ch in channels:
            f.write(ch + "\n")

@app.route('/usuarios', methods=['GET', 'POST'])
def usuarios():
    msg = None
    channels = get_channels_list()
    if request.method == 'POST':
        action = request.form.get('action')
        name = request.form.get('name', '').strip()
        old_name = request.form.get('old_name', '').strip()
        if action == 'add' and name:
            if name not in channels:
                channels.append(name)
                save_channels_list(channels)
                msg = f'Canal {name} adicionado.'
            else:
                msg = 'Canal já existe.'
        elif action == 'edit' and old_name and name:
            if old_name in channels:
                idx = channels.index(old_name)
                channels[idx] = name
                save_channels_list(channels)
                msg = f'Canal {old_name} alterado para {name}.'
            else:
                msg = 'Canal não encontrado.'
        elif action == 'delete' and old_name:
            if old_name in channels:
                channels.remove(old_name)
                save_channels_list(channels)
                msg = f'Canal {old_name} removido.'
            else:
                msg = 'Canal não encontrado.'
    return render_template_string('''
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
      <title>Gerenciar Canais</title>
      <style>body{padding:20px}</style>
    </head>
    <body>
      <div class="container">
        <h2 class="mb-4">Gerenciar Canais</h2>
        {% if msg %}<div class="alert alert-info">{{msg}}</div>{% endif %}
        <form method="post" class="mb-3">
          <div class="input-group mb-2">
            <input type="text" name="name" class="form-control" placeholder="Novo canal" required>
            <button class="btn btn-success" name="action" value="add" type="submit">Adicionar</button>
          </div>
        </form>
        <table class="table table-bordered">
          <thead><tr><th>Canal</th><th>Ações</th></tr></thead>
          <tbody>
          {% for ch in channels %}
            <tr>
              <form method="post">
                <td>
                  <input type="hidden" name="old_name" value="{{ch}}">
                  <input type="text" name="name" value="{{ch}}" class="form-control">
                </td>
                <td>
                  <button class="btn btn-primary btn-sm" name="action" value="edit" type="submit">Editar</button>
                  <button class="btn btn-danger btn-sm" name="action" value="delete" type="submit" onclick="return confirm('Remover canal?')">Remover</button>
                </td>
              </form>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        <a href="/" class="btn btn-outline-secondary">Voltar</a>
      </div>
    </body>
    </html>
    ''', channels=channels, msg=msg)

from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "kick_monitor.sqlite3")
app = Flask(__name__)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def fmt_ts(ts):
    try:
        TZ = timezone(timedelta(hours=-3))
        return datetime.fromtimestamp(int(ts), tz=TZ).strftime('%d/%m/%Y %H:%M') + ' (GMT-3)'
    except Exception:
        return str(ts)

@app.route('/')
def index():
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT DISTINCT channel FROM samples ORDER BY channel')
    channels = [r[0] for r in cur.fetchall()]
    # Picos
    peaks = {}
    cur.execute('SELECT channel, peak_overall, peak_daily, peak_weekly, peak_monthly FROM peaks')
    for r in cur.fetchall():
        peaks[r[0]] = {
            'overall': r[1], 'daily': r[2], 'weekly': r[3], 'monthly': r[4]
        }
    # Sessões recentes
    sessions = {}
    for ch in channels:
        cur.execute('SELECT id, title, start_ts, end_ts, avg_viewers, max_viewers FROM sessions WHERE channel=? ORDER BY start_ts DESC LIMIT 3', (ch,))
        sessions[ch] = [
            {
                'id': s[0], 'title': s[1], 'start': fmt_ts(s[2]), 'end': fmt_ts(s[3]) if s[3] else None,
                'avg': s[4], 'max': s[5]
            } for s in cur.fetchall()
        ]
    return render_template_string('''
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
      <title>Kick Dashboard</title>
      <style>body{padding:20px}</style>
    </head>
    <body>
      <div class="container">
        <div class="d-flex justify-content-between align-items-center mb-4">
          <h1>Kick Dashboard</h1>
          <a class="btn btn-outline-primary" href="/usuarios">Gerenciar canais</a>
        </div>
        <div class="row">
        {% for ch in channels %}
          <div class="col-md-6 col-lg-4 mb-4">
            <div class="card h-100">
              <div class="card-body">
                <h4 class="card-title">{{ch}}</h4>
                <p><a class="btn btn-sm btn-primary" href="/perfil/{{ch}}">Abrir dashboard</a></p>
                <h6>Picos</h6>
                {% if peaks.get(ch) %}
                  <ul>
                    <li>Overall: {{peaks[ch]['overall']}}</li>
                    <li>Hoje: {{peaks[ch]['daily']}}</li>
                    <li>Semana: {{peaks[ch]['weekly']}}</li>
                    <li>Mês: {{peaks[ch]['monthly']}}</li>
                  </ul>
                {% else %}<span class="text-muted">Sem dados de pico</span>{% endif %}
                <h6 class="mt-3">Sessões recentes</h6>
                {% if sessions[ch] %}
                  <ul>
                  {% for s in sessions[ch] %}
                    <li>
                      <a href="/perfil/{{ch}}?session={{s['id']}}">{{s['title'] or 'Session'}}</a>
                      <br><small>{{s['start']}} - {{s['end'] or 'em andamento'}}</small>
                      <br><small>avg: {{s['avg']}} | max: {{s['max']}}</small>
                    </li>
                  {% endfor %}
                  </ul>
                {% else %}<span class="text-muted">Sem sessões</span>{% endif %}
              </div>
            </div>
          </div>
        {% endfor %}
        </div>
      </div>
    </body>
    </html>
    ''', channels=channels, peaks=peaks, sessions=sessions)

@app.route('/perfil/<channel>')
def perfil(channel):
    db = get_db()
    cur = db.cursor()
    session_id = request.args.get('session')
    if session_id:
        cur.execute('SELECT ts, viewers FROM samples WHERE channel=? AND session_id=? ORDER BY ts ASC', (channel, session_id))
    else:
        cur.execute('SELECT ts, viewers FROM samples WHERE channel=? ORDER BY ts DESC LIMIT 200', (channel,))
    rows = cur.fetchall()[::-1]
    times = []
    viewers = []
    for ts, v in rows:
        dt = datetime.fromtimestamp(ts, timezone.utc) - timedelta(hours=3)
        times.append(dt.strftime('%d/%m %H:%M'))
        viewers.append(v)
    # Picos
    cur.execute('SELECT peak_overall, peak_daily, peak_weekly, peak_monthly FROM peaks WHERE channel=?', (channel,))
    pr = cur.fetchone() or (0,0,0,0)
    # Sessões
    cur.execute('SELECT id, title, start_ts, end_ts, avg_viewers, max_viewers FROM sessions WHERE channel=? ORDER BY start_ts DESC LIMIT 10', (channel,))
    sess = [
        {
            'id': s[0], 'title': s[1], 'start': fmt_ts(s[2]), 'end': fmt_ts(s[3]) if s[3] else None,
            'avg': s[4], 'max': s[5]
        } for s in cur.fetchall()
    ]
    return render_template_string('''
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <title>Dashboard {{channel}}</title>
      <style>body{padding:20px}</style>
    </head>
    <body>
      <div class="container">
        <div class="d-flex justify-content-between align-items-center mb-3">
          <h2>Dashboard: {{channel}}</h2>
          <a class="btn btn-sm btn-outline-secondary" href="/">Voltar</a>
        </div>
        <div class="row">
          <div class="col-md-8">
            <canvas id="myChart"></canvas>
            <script>
              const ctx = document.getElementById('myChart').getContext('2d');
              const chart = new Chart(ctx, {
                type: 'line',
                data: {
                  labels: {{ times|tojson }},
                  datasets: [{
                    label: 'Viewers',
                    data: {{ viewers|tojson }},
                    borderColor: 'rgb(13,110,253)',
                    backgroundColor: 'rgba(13,110,253,0.1)',
                    tension: 0.2
                  }]
                },
                options: { scales: { x: { display: true } }, plugins: { legend: { display: false } } }
              });
            </script>
          </div>
          <div class="col-md-4">
            <div class="card mb-3">
              <div class="card-body">
                <h5 class="card-title">Picos</h5>
                <ul>
                  <li>Overall: {{pr[0]}}</li>
                  <li>Hoje: {{pr[1]}}</li>
                  <li>Semana: {{pr[2]}}</li>
                  <li>Mês: {{pr[3]}}</li>
                </ul>
              </div>
            </div>
            <div class="card">
              <div class="card-body">
                <h5 class="card-title">Sessões recentes</h5>
                <ul>
                  {% for s in sess %}
                  <li>
                    <a href="/perfil/{{channel}}?session={{s['id']}}">{{s['title'] or 'Session'}}</a>
                    <br><small>{{s['start']}} - {{s['end'] or 'em andamento'}}</small>
                    <br><small>avg: {{s['avg']}} | max: {{s['max']}}</small>
                  </li>
                  {% endfor %}
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>
    </body>
    </html>
    ''', channel=channel, times=times, viewers=viewers, pr=pr, sess=sess)

if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "kick_monitor.sqlite3")
app = Flask(__name__)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT DISTINCT channel FROM samples ORDER BY channel')
    channels = [r[0] for r in cur.fetchall()]
    return render_template_string('''
    <html>
    <head>
      <title>Kick Dashboard</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
    </head>
    <body class="container">
      <h1 class="mt-4 mb-4">Kick Dashboard</h1>
      <h3>Canais monitorados:</h3>
      <ul>
        {% for c in channels %}
          <li><a href="/chart/{{c}}">{{c}}</a></li>
        {% endfor %}
      </ul>
    </body>
    </html>
    ''', channels=channels)

@app.route('/chart/<channel>')
def chart(channel):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT ts, viewers FROM samples WHERE channel=? ORDER BY ts DESC LIMIT 100', (channel,))
    rows = cur.fetchall()
    times = []
    viewers = []
    for ts, v in reversed(rows):
        dt = datetime.fromtimestamp(ts, timezone.utc) - timedelta(hours=3)
        times.append(dt.strftime('%d/%m %H:%M'))
        viewers.append(v)
    return render_template_string('''
    <html>
    <head>
      <title>Gráfico de {{channel}}</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
    </head>
    <body class="container">
      <h1 class="mt-4 mb-4">Gráfico de viewers: {{channel}}</h1>
      <canvas id="myChart"></canvas>
      <script>
        const ctx = document.getElementById('myChart').getContext('2d');
        const chart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: {{ times|tojson }},
            datasets: [{
              label: 'Viewers',
              data: {{ viewers|tojson }},
              borderColor: 'rgb(75, 192, 192)',
              tension: 0.1
            }]
          }
        });
      </script>
      <a href="/">Voltar</a>
    </body>
    </html>
    ''', channel=channel, times=times, viewers=viewers)

if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask, render_template_string, g, jsonify, request
import sqlite3
import os
from datetime import datetime, timezone, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "kick_monitor.sqlite3")
app = Flask(__name__)
if __name__ == '__main__':
  app.run(debug=True)
  app.run(debug=True)
  app.run(debug=True)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = sqlite3.connect(DB_PATH)
        g._database = db
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def fmt_ts(ts):
    try:
        TZ = timezone(timedelta(hours=-3))  # GMT-3
        return datetime.fromtimestamp(int(ts), tz=TZ).strftime('%Y-%m-%d %H:%M:%S') + ' (GMT-3)'
    except Exception:
        return str(ts)

INDEX_HTML = '''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <title>Kick Monitor</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <div class="d-flex justify-content-between align-items-center mb-3">
      <h1>Kick Monitor</h1>
      <div>
        <a class="btn btn-sm btn-primary" href="/">Home</a>
        <a class="btn btn-sm btn-outline-secondary" href="/peaks">Picos (JSON)</a>
      </div>
    </div>
    <div class="row">
      <div class="col-md-4">
        <div class="card mb-3">
          <div class="card-body">
            <h5 class="card-title">Canais monitorados</h5>
            <ul class="list-group">
              {% for ch in channels %}
                <li class="list-group-item d-flex justify-content-between align-items-center">
                  <a href="/chart/{{ch}}">{{ch}}</a>
                  <a class="btn btn-sm btn-outline-primary" href="/sessions/{{ch}}">Sessions</a>
                </li>
              {% endfor %}
            </ul>
          </div>
        </div>
        <div class="card">
          <div class="card-body">
            <h5 class="card-title">Picos</h5>
            {% for p in peaks %}
              <div class="mb-2">
                <strong>{{p.channel}}</strong>: overall {{p.overall}} | today {{p.daily}} | week {{p.weekly}} | month {{p.monthly}}
              </div>
            {% endfor %}
          </div>
        </div>
      </div>
      <div class="col-md-8">
        <div class="card">
          <div class="card-body">
            <h5 class="card-title">Visualização</h5>
            <p class="text-muted">Clique em um canal à esquerda para abrir o gráfico.</p>
            <div id="chart-area">
              <p class="text-muted">Último ponto e gráfico aparecerão aqui.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
'''

@app.route('/')
def index():
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT channel, peak_overall, peak_daily, peak_weekly, peak_monthly FROM peaks ORDER BY channel')
    rows = cur.fetchall()
    channels = [r[0] for r in rows]
    peaks = [{
        'channel': r[0], 'overall': r[1], 'daily': r[2], 'weekly': r[3], 'monthly': r[4]
    } for r in rows]
    return render_template_string(INDEX_HTML, channels=channels, peaks=peaks)

@app.route('/chart/<channel>')
def chart(channel):
    db = get_db()
    cur = db.cursor()
    session_id = request.args.get('session')
    if session_id:
        cur.execute('SELECT ts, viewers FROM samples WHERE channel = ? AND session_id = ? ORDER BY ts ASC', (channel, session_id))
    else:
        cur.execute('SELECT ts, viewers FROM samples WHERE channel = ? ORDER BY ts DESC LIMIT 200', (channel,))
    rows = cur.fetchall()[::-1]
    labels = [fmt_ts(r[0]) for r in rows]
    data = [r[1] for r in rows]
    cur.execute('SELECT peak_overall, peak_daily, peak_weekly, peak_monthly FROM peaks WHERE channel = ?', (channel,))
    pr = cur.fetchone() or (0, 0, 0, 0)
    return render_template_string('''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <title>Chart - {{channel}}</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <div class="d-flex justify-content-between align-items-center mb-3">
      <h2>Chart: {{channel}}</h2>
      <div>
        <a class="btn btn-sm btn-outline-secondary" href="/">Voltar</a>
        <a class="btn btn-sm btn-outline-primary" href="/sessions/{{channel}}">Ver sessions</a>
      </div>
    </div>
    <div class="row">
      <div class="col-md-8">
        <div class="d-flex justify-content-between align-items-center mb-2">
          <h5>Último: <span id="lastVal" class="badge bg-success">-</span></h5>
          <small id="lastTs" class="text-muted"></small>
        </div>
        <canvas id="c" height="120"></canvas>
      </div>
      <div class="col-md-4">
        <div class="card">
          <div class="card-body">
            <h6>Resumo de picos</h6>
            <p>Overall: <strong>{{pr[0]}}</strong></p>
            <p>Today: <strong>{{pr[1]}}</strong></p>
            <p>Week: <strong>{{pr[2]}}</strong></p>
            <p>Month: <strong>{{pr[3]}}</strong></p>
          </div>
        </div>
      </div>
    </div>
    <script>
      const labels = {{ labels|tojson }};
      const data = {{ data|tojson }};
      const ctx = document.getElementById('c').getContext('2d');
      const chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: labels,
          datasets: [{ label: 'viewers', data: data, fill: true, backgroundColor: 'rgba(13,110,253,0.1)', borderColor: 'rgba(13,110,253,1)', tension: 0.2 }]
        },
        options: { scales: { x: { display: true } }, plugins: { legend: { display: false } } }
      });
      function updateLast(val, ts) {
        const el = document.getElementById('lastVal');
        const elts = document.getElementById('lastTs');
        el.textContent = val !== null ? val : '-';
        elts.textContent = ts || '';
      }
      if (data.length && data[data.length-1] !== undefined) {
        updateLast(data[data.length-1], labels[labels.length-1]);
      }
      async function pollLatest() {
        try {
          const resp = await fetch(`/api/samples/${encodeURIComponent('{{channel}}')}?limit=1`);
          if (!resp.ok) return;
          const jr = await resp.json();
          if (jr && jr.length) {
            const s = jr[0];
            const ts = s.ts_display || s.ts;
            const v = s.viewers;
            updateLast(v, ts);
            chart.data.labels.push(ts);
            chart.data.datasets[0].data.push(v);
            if (chart.data.labels.length > 200) {
              chart.data.labels.shift();
              chart.data.datasets[0].data.shift();
            }
            chart.update();
          }
        } catch (e) {
          console.error('poll error', e);
        }
      }
      setInterval(pollLatest, 30000);
    </script>
  </div>
</body>
</html>
''', channel=channel, labels=labels, data=data, pr=pr)

@app.route('/api/samples/<channel>')
def api_samples(channel):
    db = get_db()
    cur = db.cursor()
    limit = request.args.get('limit', '200')
    try:
        limit = int(limit)
    except Exception:
        limit = 200
    cur.execute('SELECT ts, viewers FROM samples WHERE channel = ? ORDER BY ts DESC LIMIT ?', (channel, limit))
    rows = cur.fetchall()
    res = []
    for r in rows:
        ts, v = r
        res.append({'ts': ts, 'ts_display': fmt_ts(ts), 'viewers': v})
    return jsonify(res)

@app.route('/sessions/<channel>')
def sessions(channel):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT id, title, start_ts, end_ts, avg_viewers, max_viewers FROM sessions WHERE channel = ? ORDER BY start_ts DESC LIMIT 200', (channel,))
    rows = cur.fetchall()
    rows_formatted = [(r[0], r[1], fmt_ts(r[2]) if r[2] else None, fmt_ts(r[3]) if r[3] else None, r[4], r[5]) for r in rows]
    return render_template_string('''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <title>Sessions - {{channel}}</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <div class="d-flex justify-content-between align-items-center mb-3">
      <h2>Sessions: {{channel}}</h2>
      <a class="btn btn-sm btn-outline-secondary" href="/chart/{{channel}}">Voltar</a>
    </div>
    <table class="table table-sm">
      <thead><tr><th>id</th><th>title</th><th>start</th><th>end</th><th>avg</th><th>max</th><th>ver</th></tr></thead>
      <tbody>
        {% for r in rows %}
        <tr>
          <td><a href="/session/{{r[0]}}">{{r[0]}}</a></td>
          <td>{{r[1]}}</td>
          <td>{{r[2]}}</td>
          <td>{{r[3]}}</td>
          <td>{{r[4]}}</td>
          <td>{{r[5]}}</td>
          <td><a class="btn btn-sm btn-primary" href="/chart/{{channel}}?session={{r[0]}}">Ver sessão</a></td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</body>
</html>
''', channel=channel, rows=rows_formatted)

@app.route('/session/<int:session_id>')
def session_view(session_id):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT channel, title, start_ts, end_ts, avg_viewers, max_viewers FROM sessions WHERE id = ?', (session_id,))
    s = cur.fetchone()
    if not s:
        return 'Session not found', 404
    channel = s[0]
    cur.execute('SELECT ts, viewers FROM samples WHERE session_id = ? ORDER BY ts ASC', (session_id,))
    rows = cur.fetchall()
    labels = [fmt_ts(r[0]) for r in rows]
    data = [r[1] for r in rows]
    return render_template_string('''
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <title>Session {{session_id}}</title>
  <style>body{padding:20px}</style>
</head>
<body>
  <div class="container">
    <div class="d-flex justify-content-between align-items-center mb-3">
      <h2>Session {{session_id}} - {{channel}}</h2>
      <a class="btn btn-sm btn-outline-secondary" href="/sessions/{{channel}}">Voltar</a>
    </div>
    <p>title: {{title}} start={{start}} end={{end}} avg={{avg}} max={{max}}</p>
    <canvas id="c" height="120"></canvas>
    <script>
      const labels = {{ labels|tojson }};
      const data = {{ data|tojson }};
      const ctx = document.getElementById('c').getContext('2d');
      new Chart(ctx, { type: 'line', data: { labels: labels, datasets: [{ label: 'viewers', data: data, fill: true, backgroundColor: 'rgba(13,110,253,0.1)', borderColor: 'rgba(13,110,253,1)', tension: 0.2 }] } });
    </script>
  </div>
</body>
</html>
''', session_id=session_id, channel=channel, title=s[1], start=fmt_ts(s[2]) if s[2] else None, end=fmt_ts(s[3]) if s[3] else None, avg=s[4], max=s[5], labels=labels, data=data)

@app.route('/peaks')
def peaks():
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT channel, peak_overall, peak_overall_ts, peak_daily, peak_daily_date, peak_weekly, peak_week_start, peak_monthly, peak_month FROM peaks')
    rows = cur.fetchall()
    res = []
    for r in rows:
        res.append({
            'channel': r[0],
            'peak_overall': r[1],
            'peak_overall_ts': r[2],
            'peak_daily': r[3],
            'peak_daily_date': r[4],
            'peak_weekly': r[5],
            'peak_week_start': r[6],
            'peak_monthly': r[7],
            'peak_month': r[8],
        })
    return jsonify(res)

if __name__ == '__main__':
  app.run(debug=True)