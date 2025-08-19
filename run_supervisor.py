# Pequeno wrapper que reinicia `monitor.py` caso o processo termine inesperadamente.
# Uso (PowerShell): py -3 run_supervisor.py
import subprocess
import time
import sys
import os

SCRIPT = os.path.join(os.path.dirname(__file__), "monitor.py")

BACKOFF_BASE = 2

if __name__ == '__main__':
    backoff = 1
    while True:
        try:
            print("Iniciando monitor.py...")
            # Supervisor will start monitor which reads channels from DB if available
            r = subprocess.run([sys.executable, SCRIPT])
            rc = r.returncode
            print(f"monitor.py terminou com exit code {rc}")
            if rc == 0:
                print("Saída normal. Encerrando supervisor.")
                break
            else:
                wait = min(60, backoff * BACKOFF_BASE)
                print(f"Reiniciando em {wait}s...")
                time.sleep(wait)
                backoff += 1
        except KeyboardInterrupt:
            print("Supervisor interrompido pelo usuário")
            break
        except Exception as e:
            print("Erro no supervisor:", e)
            time.sleep(5)
