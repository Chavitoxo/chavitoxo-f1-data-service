CHAVITOXO F1 DATA SERVICE · V1.0.0
========================================

QUÉ ES
------
Backend independiente para Calendario F1 / Live Timing de
Chavitoxo F1 Setups.

La web NO se conecta directamente al feed F1. Este servicio:
1. negocia una conexión SignalR Core;
2. mantiene una sola WebSocket hacia F1 Live Timing;
3. recibe deltas;
4. mantiene el estado completo en RAM;
5. normaliza el snapshot al formato que espera nuestra web;
6. reparte el mismo estado a todos nuestros usuarios por REST/SSE;
7. tiene acceso controlado al archivo público de sesiones para Replay.

FUENTE DE DATOS
---------------
- Live: livetiming.formula1.com / signalrcore
- Replay/histórico: livetiming.formula1.com/static/
- NO OpenF1 Premium.
- NO Formula-Timer.
- NO Service Role de Supabase en el navegador.

TOPICS
------
Heartbeat
DriverList
ExtrapolatedClock
RaceControlMessages
SessionInfo
SessionStatus
TeamRadio
TimingAppData
TimingStats
TrackStatus
WeatherData
Position.z
CarData.z
SessionData
TimingData
TopThree
LapCount

INSTALACIÓN LOCAL
-----------------
1. Crear una carpeta, por ejemplo:
   chavitoxo-f1-data-service

2. Colocar dentro:
   server.js
   package.json
   .env

3. En CMD/PowerShell dentro de la carpeta:
   npm install

4. Iniciar:
   npm start

5. Abrir:
   http://127.0.0.1:3000/health

Aunque no haya una sesión de F1 en ese momento, el servidor debe
quedar funcionando y mostrar el estado de la conexión.

ENDPOINTS
---------
GET /
GET /health
GET /api/live/status
GET /api/live/snapshot
GET /api/live/raw
GET /api/live/stream

ARCHIVO / REPLAY
----------------
GET /api/archive/2026

Devuelve Index.json del archivo 2026.

GET /api/archive/file?path=RUTA

La ruta se valida para impedir ../ y caracteres peligrosos.

FORMATO PRINCIPAL PARA NUESTRA WEB
----------------------------------
GET /api/live/snapshot

Respuesta:
{
  "success": true,
  "source": "f1_livetiming",
  "connected": true/false,
  "meeting_name": "...",
  "session_name": "...",
  "session_status": "...",
  "track_status": "...",
  "clock": "...",
  "lap": 1,
  "total_laps": 57,
  "weather": {...},
  "drivers": [...],
  "race_control": [...],
  "penalties": [...],
  "team_radio": [...]
}

SSE
---
GET /api/live/stream

Cada cliente recibe:
event: snapshot
data: {...}

El servidor limita la emisión del snapshot aproximadamente a 4 Hz
para no inundar el navegador.

ESTABILIDAD
-----------
- reconexión automática exponencial de 2 a 30 segundos;
- keepalive SignalR cada 15 s;
- F1 se conecta UNA vez, no una vez por usuario;
- si Live falla, el resto de Chavitoxo F1 Setups no depende de este proceso;
- CORS restringido a localhost y chavitoxof1setups.com;
- la ruta del archivo histórico está validada.

IMPORTANTE PARA PRODUCCIÓN
--------------------------
Este proceso debe ejecutarse en un hosting que permita:
- Node.js persistente;
- conexiones WebSocket salientes;
- conexiones HTTP/SSE largas;
- proceso 24/7 durante los GP.

Una Supabase Edge Function NO es el lugar ideal para mantener una
WebSocket persistente. Mantendremos Supabase para la app y usaremos
este servicio exclusivamente como gateway de Live Timing.

SIGUIENTE ETAPA
---------------
Conectar app.js V10.15.0 a:
- /health
- /api/live/stream
- /api/live/snapshot

Luego:
1. Timing real.
2. Sectores + minisectores.
3. Neumáticos/stints.
4. Race Control.
5. Team Radio.
6. Posiciones/mapa cuando la fuente las entregue.
7. Replay desde archivo.
8. Analytics calculados por nosotros.
