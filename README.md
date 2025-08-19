# Kick Live Monitor

Pequeno utilitário em Python para coletar o número de espectadores (viewers) ao vivo de canais da Kick e salvar amostras em SQLite.

Como usar:

- Edite `channels.txt` adicionando um slug de canal por linha (ex: `xqc`).
- Rode `python monitor.py --once` para coletar uma vez e mostrar o resultado.
- Rode `python monitor.py` para iniciar o monitoramento contínuo (coleta a cada 30s por canal).

Os dados são salvos em `kick_monitor.sqlite3` na mesma pasta.
