Node dashboard (Express) for FDS bot

Quick start (PowerShell):

cd C:\Users\Sam\Documents\programa\fds_bot\web-dashboard; npm install
cd C:\Users\Sam\Documents\programa\fds_bot\web-dashboard; npm start

Open http://127.0.0.1:3000/

Notes:
- Uses the existing SQLite DB at ../fds_bot.db (creates if missing).
- API endpoints: GET /api/channels, POST /api/channels, PUT /api/channels/:id, DELETE /api/channels/:id
