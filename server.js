/*
=========================================================
CHAVITOXO F1 DATA SERVICE
V1.41.0 · REPLAY QUALI CHECKERED LAP COMPLETION
Node.js 20+ / 22+

Objetivo:
- Una sola conexión servidor -> F1 Live Timing.
- El navegador NUNCA se conecta directamente a F1.
- Estado en memoria con delta-merge.
- Reconexión automática.
- Snapshot REST + stream SSE para Chavitoxo F1 Setups.
- Acceso controlado al archivo público de sesiones para Replay.

IMPORTANTE:
Este servicio no usa OpenF1 de pago ni endpoints privados
de Formula-Timer.
=========================================================
*/

"use strict";

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");
const zlib = require("zlib");
const { EventEmitter } = require("events");

const PORT = Number(process.env.PORT || 3000);
const F1_HOST = "livetiming.formula1.com";
const SIGNALR_PATH = "/signalrcore";
const RECORD_SEPARATOR = "\x1e";
const F1_STATIC_BASE = "https://livetiming.formula1.com/static";

const ALLOWED_ORIGINS = String(
    process.env.ALLOWED_ORIGINS ||
    "http://127.0.0.1:5500,http://localhost:5500,https://chavitoxof1setups.com"
)
    .split(",")
    .map(value => value.trim())
    .filter(Boolean);

const TOPICS = [
    "Heartbeat",
    "DriverList",
    "ExtrapolatedClock",
    "RaceControlMessages",
    "SessionInfo",
    "SessionStatus",
    "TeamRadio",
    "TimingAppData",
    "TimingStats",
    "TrackStatus",
    "WeatherData",
    "Position.z",
    "CarData.z",
    "SessionData",
    "TimingData",
    "TimingDataF1",
    "TopThree",
    "LapCount",
    "PitLaneTimeCollection",
    "TyreStintSeries",
    "DriverRaceInfo"
];

const state = {
    connected: false,
    phase: "idle",
    last_message_at: null,
    last_connected_at: null,
    last_disconnected_at: null,
    last_error: null,
    reconnect_attempt: 0,
    topics: Object.create(null)
};

const events = new EventEmitter();
events.setMaxListeners(0);

let f1Socket = null;
let reconnectTimer = null;
let reconnectDelayMs = 2000;

let connectionWatchdogTimer = null;
let lastSocketActivityAt = 0;
const F1_SOCKET_STALE_MS = 30000;

const liveTopicDiagnostics = {
    position_messages: 0,
    position_samples: 0,
    position_decode_errors: 0,
    last_position_at: null,
    last_position_sample_count: 0
};
let keepAliveTimer = null;
let stopped = false;
let lastSseBroadcastAt = 0;
let sseBroadcastTimer = null;

/*
   V1.20.0 · LIVE TRACK LEARNING
   Aprende el trazado directamente de Position.z cuando el archivo
   histórico de la sesión todavía no está disponible.
*/
let liveTrackSessionKey = "";
const liveTrackTrajectories = new Map();
const liveLatestPositions = new Map();

/*
   V1.23.0 · Últimos valores válidos del Live Timing.
   F1 a veces vacía Gap/Interval al cerrar la sesión. Conservamos
   el último valor oficial recibido para que la clasificación final
   no pierda información al terminar.
*/
let liveDriverDisplaySessionKey = "";
const liveDriverDisplayCache = new Map();


/* =========================================================
   UTILIDADES
========================================================= */

function isPlainObject(value) {
    return Boolean(
        value &&
        typeof value === "object" &&
        !Array.isArray(value)
    );
}

function cloneJson(value) {
    if (value === undefined) return undefined;
    return JSON.parse(JSON.stringify(value));
}

function deepMerge(target, patch) {
    /*
       F1 SignalR envía DELTAS, no snapshots completos.
       Arrays como Sectors, Segments y Stints también pueden llegar
       parcialmente. Reemplazar el array completo con cada delta
       destruye datos que ya estaban en RAM.
    */
    if (Array.isArray(patch)) {
        const output =
            Array.isArray(target)
                ? target.map(cloneJson)
                : [];

        for (
            let index = 0;
            index < patch.length;
            index++
        ) {
            const value =
                patch[index];

            if (value === undefined) {
                continue;
            }

            if (
                isPlainObject(value) ||
                Array.isArray(value)
            ) {
                output[index] =
                    deepMerge(
                        output[index],
                        value
                    );
            } else {
                output[index] =
                    cloneJson(value);
            }
        }

        return output;
    }

    if (!isPlainObject(patch)) {
        return cloneJson(patch);
    }

    const output =
        isPlainObject(target)
            ? { ...target }
            : {};

    for (
        const [key, value]
        of Object.entries(patch)
    ) {
        if (
            isPlainObject(value) ||
            Array.isArray(value)
        ) {
            output[key] =
                deepMerge(
                    output[key],
                    value
                );
        } else {
            output[key] =
                cloneJson(value);
        }
    }

    return output;
}

function decodeCompressedTopic(value) {
    if (typeof value !== "string") {
        return value;
    }

    const compressed =
        Buffer.from(
            value,
            "base64"
        );

    const attempts = [
        () =>
            zlib.inflateRawSync(
                compressed
            ),
        () =>
            zlib.inflateSync(
                compressed
            )
    ];

    for (const attempt of attempts) {
        try {
            const inflated =
                attempt();

            return JSON.parse(
                inflated.toString(
                    "utf8"
                )
            );
        } catch {
            // try next decoder
        }
    }

    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}

function cleanTopicName(topic) {
    return String(topic).endsWith(".z")
        ? String(topic).slice(0, -2)
        : String(topic);
}


function currentLiveTrackSessionKey() {
    const sessionInfo =
        state.topics.SessionInfo ||
        {};

    const rawPath =
        String(
            sessionInfo.Path ||
            sessionInfo.path ||
            ""
        )
        .trim();

    /*
       V1.29.0
       El circuito es el mismo durante todo el fin de semana.
       Usamos la carpeta del MEETING, no la sesión individual,
       para no borrar el mapa al pasar FP2 -> Qualifying -> Race.
    */
    if (rawPath) {
        const clean =
            rawPath.replace(
                /\/+$/,
                ""
            );

        const parts =
            clean.split("/");

        if (parts.length >= 2) {
            parts.pop();

            return (
                parts.join("/") +
                "/"
            );
        }
    }

    return String(
        sessionInfo?.Meeting?.Name ||
        sessionInfo?.Meeting?.OfficialName ||
        sessionInfo.MeetingName ||
        "live-weekend"
    );
}

function resetLiveTrackLearningIfNeeded() {
    const key =
        currentLiveTrackSessionKey();

    if (
        key &&
        key !== liveTrackSessionKey
    ) {
        liveTrackSessionKey =
            key;

        liveTrackTrajectories.clear();
    }
}

function appendLiveTrackPoint(
    driverNumber,
    x,
    y
) {
    const number =
        String(
            driverNumber ||
            ""
        );

    if (
        !number ||
        !Number.isFinite(x) ||
        !Number.isFinite(y)
    ) {
        return;
    }

    if (!liveTrackTrajectories.has(number)) {
        liveTrackTrajectories.set(
            number,
            []
        );
    }

    const points =
        liveTrackTrajectories.get(
            number
        );

    const last =
        points[
            points.length - 1
        ];

    if (last) {
        const distance =
            Math.hypot(
                x - last.x,
                y - last.y
            );

        /*
           Evita duplicados exactos y saltos claramente corruptos.
           Los límites son deliberadamente amplios porque la escala
           de Position.z puede variar por sesión.
        */
        if (distance < 0.01) {
            return;
        }

        if (distance > 10000) {
            return;
        }
    }

    points.push({
        x,
        y
    });

    if (points.length > 12000) {
        points.splice(
            0,
            points.length - 12000
        );
    }
}


function extractLivePositionSamples(rawData) {
    const samples = [];
    const seen = new Set();

    const pushSample = (number, car, timestamp = null) => {
        if (!car || typeof car !== "object") return;

        const driverNumber = String(
            car.RacingNumber ??
            car.DriverNumber ??
            car.Number ??
            number ??
            ""
        ).trim();

        const x = Number(car.X ?? car.x);
        const y = Number(car.Y ?? car.y);
        const z = Number(car.Z ?? car.z);

        if (!driverNumber || !Number.isFinite(x) || !Number.isFinite(y)) {
            return;
        }

        const key = `${driverNumber}|${x}|${y}|${timestamp || ""}`;
        if (seen.has(key)) return;
        seen.add(key);

        samples.push({
            driver_number: driverNumber,
            x,
            y,
            z: Number.isFinite(z) ? z : null,
            status: car.Status ?? car.status ?? null,
            timestamp:
                timestamp ??
                car.Timestamp ??
                car.Utc ??
                car.timestamp ??
                null
        });
    };

    const queue = [{ value: rawData, timestamp: null, numericKey: null }];
    const visited = new Set();

    while (queue.length) {
        const current = queue.shift();
        const value = current.value;

        if (!value || typeof value !== "object" || visited.has(value)) {
            continue;
        }

        visited.add(value);

        const timestamp =
            value.Timestamp ??
            value.Utc ??
            value.timestamp ??
            current.timestamp ??
            null;

        for (const [key, child] of Object.entries(value)) {
            if (
                /^\d+$/.test(String(key)) &&
                child &&
                typeof child === "object"
            ) {
                pushSample(key, child, timestamp);
            }
        }

        if (
            value.RacingNumber !== undefined ||
            value.DriverNumber !== undefined ||
            value.Number !== undefined
        ) {
            pushSample(current.numericKey, value, timestamp);
        }

        for (const [key, child] of Object.entries(value)) {
            if (!child || typeof child !== "object") continue;

            if (Array.isArray(child)) {
                child.forEach(item => {
                    if (item && typeof item === "object") {
                        queue.push({
                            value: item,
                            timestamp,
                            numericKey: current.numericKey
                        });
                    }
                });
            } else {
                queue.push({
                    value: child,
                    timestamp,
                    numericKey: /^\d+$/.test(String(key))
                        ? String(key)
                        : current.numericKey
                });
            }
        }
    }

    return samples;
}

function learnLiveTrackFromPosition(rawData) {
    resetLiveTrackLearningIfNeeded();

    for (const sample of extractLivePositionSamples(rawData)) {
        appendLiveTrackPoint(
            sample.driver_number,
            sample.x,
            sample.y
        );
    }
}

function simplifyLiveTrackTrajectory(
    points,
    maxPoints = 1800
) {
    if (
        !Array.isArray(points) ||
        points.length <= maxPoints
    ) {
        return points || [];
    }

    const step =
        Math.max(
            1,
            Math.ceil(
                points.length /
                maxPoints
            )
        );

    const output = [];

    for (
        let index = 0;
        index < points.length;
        index += step
    ) {
        output.push(
            points[index]
        );
    }

    if (
        points.length &&
        output[
            output.length - 1
        ] !==
        points[
            points.length - 1
        ]
    ) {
        output.push(
            points[
                points.length - 1
            ]
        );
    }

    return output;
}

function buildLiveTrackGeometrySnapshot() {
    const candidates =
        Array.from(
            liveTrackTrajectories.entries()
        )
        .map(
            ([number, points]) => ({
                number,
                points
            })
        )
        .filter(
            item =>
                item.points.length >= 20
        )
        .sort(
            (a,b) =>
                b.points.length -
                a.points.length
        );

    if (!candidates.length) {
        return {
            success:false,
            learning:true,
            session_key:
                liveTrackSessionKey,
            points:[],
            segments:[],
            bounds:null
        };
    }

    /*
       Usamos el recorrido más completo disponible.
       Para una sesión activa, después de una vuelta ya produce
       el circuito completo; las vueltas posteriores refuerzan
       la misma silueta.
    */
    const best =
        candidates[0];

    const segment =
        simplifyLiveTrackTrajectory(
            best.points
        );

    const bounds =
        getTrackGeometryBounds(
            segment
        );

    return {
        success:true,
        learning:false,
        session_key:
            liveTrackSessionKey,
        source_driver:
            best.number,
        learned_points:
            best.points.length,
        points:
            segment,
        segments:
            [segment],
        sector_segments:[],
        bounds
    };
}

function applyTopicUpdate(topic, rawData) {
    const compressed = String(topic).endsWith(".z");
    const cleanTopic = cleanTopicName(topic);
    const data = compressed
        ? decodeCompressedTopic(rawData)
        : rawData;

    if (data === undefined || data === null) {
        return;
    }

    state.topics[cleanTopic] = deepMerge(
        state.topics[cleanTopic],
        data
    );

    if (cleanTopic === "SessionInfo") {
        resetLiveTrackLearningIfNeeded();
    }

    if (cleanTopic === "Position") {
        liveTopicDiagnostics.position_messages +=
            1;

        liveTopicDiagnostics.last_position_at =
            new Date().toISOString();

        const patchSamples =
            extractLivePositionSamples(
                data
            );

        const accumulatedSamples =
            extractLivePositionSamples(
                state.topics.Position
            );

        const samplesByDriver =
            new Map();

        for (
            const sample of [
                ...accumulatedSamples,
                ...patchSamples
            ]
        ) {
            const number =
                String(
                    sample.driver_number ||
                    ""
                ).trim();

            if (!number) continue;

            samplesByDriver.set(
                number,
                sample
            );
        }

        liveTopicDiagnostics.last_position_sample_count =
            samplesByDriver.size;

        liveTopicDiagnostics.position_samples +=
            patchSamples.length;

        for (
            const [
                number,
                sample
            ] of samplesByDriver.entries()
        ) {
            const x =
                Number(sample.x);

            const y =
                Number(sample.y);

            if (
                !Number.isFinite(x) ||
                !Number.isFinite(y)
            ) {
                continue;
            }

            liveLatestPositions.set(
                number,
                {
                    driver_number:
                        number,
                    x,
                    y,
                    z:
                        Number.isFinite(
                            Number(
                                sample.z
                            )
                        )
                            ? Number(
                                sample.z
                            )
                            : null,
                    status:
                        sample.status ??
                        null,
                    timestamp:
                        sample.timestamp ??
                        null
                }
            );
        }

        learnLiveTrackFromPosition(
            state.topics.Position
        );
    }

    state.last_message_at = new Date().toISOString();

    events.emit("topic", {
        topic: cleanTopic,
        data,
        received_at: state.last_message_at
    });

    scheduleSseSnapshotBroadcast();
}

function scheduleSseSnapshotBroadcast() {
    const now = Date.now();
    const minimumInterval = 250;

    if (now - lastSseBroadcastAt >= minimumInterval) {
        lastSseBroadcastAt = now;
        events.emit("snapshot", buildNormalizedSnapshot());
        return;
    }

    if (sseBroadcastTimer) {
        return;
    }

    sseBroadcastTimer = setTimeout(() => {
        sseBroadcastTimer = null;
        lastSseBroadcastAt = Date.now();
        events.emit("snapshot", buildNormalizedSnapshot());
    }, minimumInterval);
}

function asArray(value) {
    if (Array.isArray(value)) {
        return value;
    }

    if (isPlainObject(value)) {
        return Object.entries(value)
            .sort((a, b) => {
                const na = Number(a[0]);
                const nb = Number(b[0]);

                if (Number.isFinite(na) && Number.isFinite(nb)) {
                    return na - nb;
                }

                return String(a[0]).localeCompare(String(b[0]));
            })
            .map(([, item]) => item);
    }

    return [];
}

function valueOfTime(value) {
    if (value === null || value === undefined) {
        return null;
    }

    if (typeof value === "string" || typeof value === "number") {
        return value;
    }

    return (
        value.Value ??
        value.value ??
        value.Time ??
        value.time ??
        null
    );
}

function normalizeTeamColour(value) {
    const text = String(value || "").trim().replace("#", "");
    return /^[0-9a-f]{6}$/i.test(text)
        ? "#" + text.toUpperCase()
        : null;
}

function topic(name) {
    return state.topics[name] || {};
}


/* =========================================================
   NORMALIZADOR CHAVITOXO
========================================================= */

function getDriverMap() {
    const raw = topic("DriverList");
    const result = Object.create(null);

    for (const [number, driver] of Object.entries(raw)) {
        if (!driver || typeof driver !== "object") continue;

        const driverNumber = String(
            driver.RacingNumber ||
            driver.Number ||
            number
        );

        result[driverNumber] = {
            number: driverNumber,
            name:
                driver.FullName ||
                driver.BroadcastName ||
                driver.LastName ||
                driver.Tla ||
                driverNumber,
            abbreviation:
                driver.Tla ||
                driver.ShortName ||
                driver.LastName ||
                driverNumber,
            team:
                driver.TeamName ||
                driver.Team ||
                "",
            team_color:
                normalizeTeamColour(
                    driver.TeamColour ||
                    driver.TeamColor
                ),
            country_code:
                driver.CountryCode ||
                "",
            headshot_url:
                driver.HeadshotUrl ||
                driver.HeadshotURL ||
                null
        };
    }

    return result;
}

function getCurrentStint(number) {
    const timingApp = topic("TimingAppData");
    const line =
        timingApp?.Lines?.[number] ||
        timingApp?.lines?.[number] ||
        null;

    if (!line) {
        return null;
    }

    const stints = asArray(
        line.Stints ||
        line.stints ||
        []
    );

    return stints.length
        ? stints[stints.length - 1]
        : null;
}

function getDriverPositionsNormalized() {
    const raw =
        topic("Position");

    const samples =
        extractLivePositionSamples(
            raw
        );

    const latestByDriver =
        Object.create(null);

    for (const [number, position] of liveLatestPositions.entries()) {
        if (
            Number.isFinite(Number(position?.x)) &&
            Number.isFinite(Number(position?.y))
        ) {
            latestByDriver[String(number)] = {
                driver_number: String(number),
                x: Number(position.x),
                y: Number(position.y),
                z: Number.isFinite(Number(position?.z)) ? Number(position.z) : null,
                status: position?.status ?? null,
                timestamp: position?.timestamp ?? null
            };
        }
    }

    for (const sample of samples) {
        const number =
            String(
                sample.driver_number ||
                ""
            ).trim();

        if (!number) continue;

        const previous =
            latestByDriver[number];

        const previousTime =
            previous?.timestamp
                ? new Date(previous.timestamp).getTime()
                : NaN;

        const sampleTime =
            sample?.timestamp
                ? new Date(sample.timestamp).getTime()
                : NaN;

        if (
            !previous ||
            !Number.isFinite(previousTime) ||
            !Number.isFinite(sampleTime) ||
            sampleTime >= previousTime
        ) {
            latestByDriver[number] = {
                driver_number: number,
                x: sample.x,
                y: sample.y,
                z: sample.z,
                status: sample.status,
                timestamp: sample.timestamp
            };
        }
    }

    /*
       Position.z es diferencial. Si el árbol acumulado no entrega
       una posición actual para un piloto, reutilizamos el último
       punto REAL recibido por el aprendiz del trazado.
    */
    for (const [number, points] of liveTrackTrajectories.entries()) {
        if (
            latestByDriver[number] ||
            !Array.isArray(points) ||
            !points.length
        ) {
            continue;
        }

        const point =
            points[points.length - 1];

        if (
            Number.isFinite(Number(point?.x)) &&
            Number.isFinite(Number(point?.y))
        ) {
            latestByDriver[number] = {
                driver_number: String(number),
                x: Number(point.x),
                y: Number(point.y),
                z: null,
                status: null,
                timestamp: null
            };
        }
    }

    return latestByDriver;
}


function normalizeLiveBoolean(value) {
    if (value === true || value === 1) {
        return true;
    }

    if (
        value === false ||
        value === 0 ||
        value === null ||
        value === undefined
    ) {
        return false;
    }

    const text =
        String(value)
            .trim()
            .toLowerCase();

    if (
        ["true","1","yes","on"].includes(text)
    ) {
        return true;
    }

    if (
        ["false","0","no","off",""].includes(text)
    ) {
        return false;
    }

    return Boolean(value);
}

function normalizeLiveDisplayValue(value) {
    const normalized =
        valueOfTime(value);

    if (
        normalized === null ||
        normalized === undefined
    ) {
        return null;
    }

    const text =
        String(normalized)
            .trim();

    if (
        !text ||
        text === "-" ||
        text === "--" ||
        text === "—"
    ) {
        return null;
    }

    return normalized;
}

function getLiveDriverDisplaySessionKey() {
    const info =
        topic("SessionInfo") ||
        {};

    return String(
        info.Path ||
        info.path ||
        [
            info?.Meeting?.Name ||
            info?.Meeting?.OfficialName ||
            "",
            info.Name ||
            info.SessionName ||
            ""
        ]
        .filter(Boolean)
        .join("|")
    );
}

function resetLiveDriverDisplayCacheIfNeeded() {
    const key =
        getLiveDriverDisplaySessionKey();

    if (
        key &&
        key !== liveDriverDisplaySessionKey
    ) {
        liveDriverDisplaySessionKey =
            key;

        liveDriverDisplayCache.clear();
    }
}

function preserveLiveDriverDisplayValue(
    driverNumber,
    field,
    value
) {
    resetLiveDriverDisplayCacheIfNeeded();

    const number =
        String(driverNumber);

    if (!liveDriverDisplayCache.has(number)) {
        liveDriverDisplayCache.set(
            number,
            {}
        );
    }

    const cache =
        liveDriverDisplayCache.get(
            number
        );

    const normalized =
        normalizeLiveDisplayValue(
            value
        );

    if (
        normalized !== null &&
        normalized !== undefined
    ) {
        cache[field] =
            normalized;

        return normalized;
    }

    return (
        cache[field] ??
        null
    );
}


function normalizeTimingStatsValue(
    value
) {
    if (
        value === null ||
        value === undefined
    ) {
        return null;
    }

    if (
        typeof value === "string" ||
        typeof value === "number"
    ) {
        const text =
            String(value).trim();

        return text || null;
    }

    if (
        typeof value === "object"
    ) {
        const nested =
            value.Value ??
            value.value ??
            value.Time ??
            value.time ??
            null;

        if (
            nested !== null &&
            nested !== undefined
        ) {
            const text =
                String(nested).trim();

            return text || null;
        }
    }

    return null;
}


function getTimingStatsByDriver() {
    const raw =
        topic("TimingStats") ||
        {};

    const rawLines =
        raw.Lines ??
        raw.lines ??
        raw;

    const result =
        {};

    /*
       TimingStats puede llegar como:
       - array
       - objeto con claves 0,1,2...
       - objeto indexado por RacingNumber

       No asumimos una sola forma.
    */
    const entries =
        Array.isArray(rawLines)
            ? rawLines.map(
                (line, index) => [
                    String(index),
                    line
                ]
            )
            : Object.entries(
                rawLines &&
                typeof rawLines === "object"
                    ? rawLines
                    : {}
            );

    entries.forEach(
        ([key, line]) => {
            if (
                !line ||
                typeof line !== "object"
            ) {
                return;
            }

            const racingNumber =
                String(
                    line.RacingNumber ??
                    line.racingNumber ??
                    line.DriverNumber ??
                    line.driverNumber ??
                    key ??
                    ""
                )
                .trim();

            if (!racingNumber) {
                return;
            }

            const rawBestSectors =
                line.BestSectors ??
                line.bestSectors ??
                [];

            const bestSectors =
                asArray(
                    rawBestSectors
                )
                .slice(
                    0,
                    3
                )
                .map(
                    sector =>
                        normalizeTimingStatsValue(
                            sector
                        )
                );

            /*
               Algunos feeds usan objeto {"0":...,"1":...,"2":...}.
               asArray ya lo normaliza si es objeto numérico.
            */
            result[racingNumber] = {
                racing_number:
                    racingNumber,

                best_lap:
                    normalizeTimingStatsValue(
                        line.PersonalBestLapTime ??
                        line.personalBestLapTime
                    ),

                best_sector_1:
                    bestSectors[0] ??
                    null,

                best_sector_2:
                    bestSectors[1] ??
                    null,

                best_sector_3:
                    bestSectors[2] ??
                    null,

                best_speeds:
                    line.BestSpeeds ??
                    line.bestSpeeds ??
                    null
            };
        }
    );

    return result;
}



function getF1QualifyingStatsArray(line) {
    const stats =
        line?.Stats ??
        line?.stats ??
        null;

    if (Array.isArray(stats)) {
        return stats;
    }

    if (stats && typeof stats === "object") {
        const numericKeys =
            Object.keys(stats)
                .filter(key => /^\\d+$/.test(String(key)))
                .sort((a,b) => Number(a) - Number(b));

        if (numericKeys.length) {
            return numericKeys.map(key => stats[key]);
        }

        return [stats];
    }

    return [];
}

function getF1QualifyingStat(line) {
    const stats =
        getF1QualifyingStatsArray(line);

    if (!stats.length) {
        return null;
    }

    /*
       F1 mantiene Stats por cada parte de Qualifying:
       Stats[0] = Q1
       Stats[1] = Q2
       Stats[2] = Q3

       La fase actual/final ya la determina getLiveQualifyingPhase().
       Si la sesión está en Q3 debemos leer Stats[2], no Stats[0].
       Para pilotos eliminados que no tienen valor en la fase actual,
       buscamos hacia atrás su última fase válida.
    */
    const phase =
        String(
            getLiveQualifyingPhase() ||
            ""
        )
        .trim()
        .toUpperCase();

    const requestedIndex =
        /3$/.test(phase)
            ? 2
            : (
                /2$/.test(phase)
                    ? 1
                    : 0
            );

    const hasUsefulValue = stat => {
        if (!stat || typeof stat !== "object") {
            return false;
        }

        return [
            stat.TimeDiffToFastest,
            stat.timeDiffToFastest,
            stat.TimeDifftoPositionAhead,
            stat.TimeDiffToPositionAhead,
            stat.timeDifftoPositionAhead,
            stat.timeDiffToPositionAhead
        ].some(value => {
            const normalized =
                valueOfTime(value);

            return (
                normalized !== null &&
                normalized !== undefined &&
                String(normalized).trim() !== ""
            );
        });
    };

    for (
        let index =
            Math.min(
                requestedIndex,
                stats.length - 1
            );
        index >= 0;
        index--
    ) {
        if (hasUsefulValue(stats[index])) {
            return stats[index];
        }
    }

    return (
        stats[requestedIndex] ||
        stats[stats.length - 1] ||
        stats[0] ||
        null
    );
}

function getF1QualifyingGap(line) {
    const stat =
        getF1QualifyingStat(line);

    return valueOfTime(
        line?.GapToLeader ??
        line?.Gap ??
        line?.TimeDiffToFastest ??
        stat?.TimeDiffToFastest ??
        stat?.timeDiffToFastest
    );
}

function getF1QualifyingInterval(line) {
    const stat =
        getF1QualifyingStat(line);

    return valueOfTime(
        line?.IntervalToPositionAhead ??
        line?.Interval ??
        line?.TimeDiffToPositionAhead ??
        stat?.TimeDifftoPositionAhead ??
        stat?.TimeDiffToPositionAhead ??
        stat?.timeDifftoPositionAhead ??
        stat?.timeDiffToPositionAhead
    );
}



function getOfficialBestSectorsForDriver(number) {
    const statsTopic =
        state.topics.TimingStats ||
        {};

    const lines =
        statsTopic.Lines ||
        statsTopic.lines ||
        statsTopic;

    const row =
        lines?.[number] ||
        lines?.[String(number)] ||
        null;

    const best =
        row?.BestSectors ??
        row?.bestSectors ??
        null;

    const readSector = index => {
        const item =
            Array.isArray(best)
                ? best[index]
                : (
                    best?.[index] ??
                    best?.[String(index)] ??
                    best?.[index + 1] ??
                    best?.[String(index + 1)]
                );

        return valueOf(
            item?.Value ??
            item?.value ??
            item
        );
    };

    return [
        readSector(0),
        readSector(1),
        readSector(2)
    ];
}


function getDriversNormalized() {
    const timingClassic =
        topic("TimingData") ||
        {};

    const timingF1 =
        topic("TimingDataF1") ||
        {};

    /*
       V1.26.0
       F1 puede publicar GAP / INTERVAL y otros campos en
       TimingDataF1 aunque TimingData no los esté exponiendo
       en ese instante. Combinamos ambos feeds por piloto y
       damos prioridad a TimingDataF1 cuando contiene el campo.
    */
    const classicLines =
        timingClassic.Lines ||
        timingClassic.lines ||
        {};

    const f1Lines =
        timingF1.Lines ||
        timingF1.lines ||
        {};

    const allNumbers =
        new Set([
            ...Object.keys(
                classicLines
            ),
            ...Object.keys(
                f1Lines
            )
        ]);

    const lines =
        Object.fromEntries(
            Array.from(
                allNumbers
            )
            .map(
                number => [
                    number,
                    deepMerge(
                        classicLines[number] || {},
                        f1Lines[number] || {}
                    )
                ]
            )
        );

    const driverMap =
        getDriverMap();

    const timingStatsMap =
        getTimingStatsByDriver();

    const positionMap =
        getDriverPositionsNormalized();

    const normalizedRows =
        Object.entries(lines)
        .map(([number, line]) => {
            if (
                !line ||
                typeof line !== "object"
            ) {
                return null;
            }

            const driver =
                driverMap[String(number)] ||
                {
                    number:
                        String(number),
                    name:
                        String(number),
                    abbreviation:
                        String(number),
                    team:"",
                    team_color:null
                };

            const timingStats =
                timingStatsMap[
                    String(number)
                ] ||
                {};

            const sectors =
                asArray(
                    line.Sectors ||
                    line.sectors ||
                    []
                );

            const stint =
                getCurrentStint(
                    String(number)
                );

            const position =
                line.Position ??
                line.position ??
                null;

            /*
               V1.37.0 · QUALIFYING REAL
               V1.36 ya tenía los helpers de Stats[0], pero este bloque
               seguía leyendo únicamente los campos top-level. Por eso
               INT / LÍDER continuaban vacíos aunque F1 sí enviara Stats.
            */
            const interval =
                preserveLiveDriverDisplayValue(
                    number,
                    "interval",
                    getF1QualifyingInterval(line)
                );

            const gap =
                preserveLiveDriverDisplayValue(
                    number,
                    "gap",
                    getF1QualifyingGap(line)
                );

            const lastLap =
                preserveLiveDriverDisplayValue(
                    number,
                    "last_lap",
                    line.LastLapTime ??
                    line.lastLapTime
                );

            const bestLap =
                preserveLiveDriverDisplayValue(
                    number,
                    "best_lap",
                    line.BestLapTime ??
                    line.bestLapTime
                );

            const normalizedSectors =
                sectors.map(
                    (sector, index) => {
                        const sectorValue =
                            preserveLiveDriverDisplayValue(
                                number,
                                "sector_" +
                                String(index + 1),
                                sector
                            );

                        return {
                            index:
                                index + 1,

                            value:
                                sectorValue,

                            personal_best:
                                normalizeLiveBoolean(
                                    sector?.PersonalFastest ??
                                    sector?.personalFastest
                                ),

                            overall_best:
                                normalizeLiveBoolean(
                                    sector?.OverallFastest ??
                                    sector?.overallFastest
                                ),

                            segments:
                                asArray(
                                    sector?.Segments ||
                                    sector?.segments ||
                                    []
                                )
                        };
                    }
                );

            return {
                driver_number:
                    String(number),

                name:
                    driver.name,

                abbreviation:
                    driver.abbreviation,

                team:
                    driver.team,

                team_color:
                    driver.team_color,

                country_code:
                    driver.country_code ||
                    "",

                headshot_url:
                    driver.headshot_url ||
                    null,

                map_position:
                    positionMap[String(number)] ||
                    null,

                position,

                completed_laps:
                    Number(
                        line.NumberOfLaps ??
                        line.numberOfLaps ??
                        line.LapNumber ??
                        line.lapNumber ??
                        0
                    ) || 0,

                gap,

                interval,

                last_lap:
                    lastLap,

                best_lap:
                    bestLap,

                sector_1:
                    normalizedSectors[0]?.value ??
                    null,

                sector_2:
                    normalizedSectors[1]?.value ??
                    null,

                sector_3:
                    normalizedSectors[2]?.value ??
                    null,

                /*
                   V1.28.0
                   IMPORTANTE:
                   sector_1/2/3 = sector actual / último reportado en TimingData.
                   best_sector_1/2/3 = mejor sector PERSONAL oficial de la sesión,
                   proveniente de TimingStats.BestSectors.
                */
                best_sector_1:
                    preserveLiveDriverDisplayValue(
                        number,
                        "best_sector_1",
                        timingStats.best_sector_1
                    ),

                best_sector_2:
                    preserveLiveDriverDisplayValue(
                        number,
                        "best_sector_2",
                        timingStats.best_sector_2
                    ),

                best_sector_3:
                    preserveLiveDriverDisplayValue(
                        number,
                        "best_sector_3",
                        timingStats.best_sector_3
                    ),

                timing_stats_best_lap:
                    preserveLiveDriverDisplayValue(
                        number,
                        "timing_stats_best_lap",
                        timingStats.best_lap
                    ),

                sectors:
                    normalizedSectors,

                compound:
                    stint?.Compound ??
                    stint?.compound ??
                    null,

                tyre_age:
                    stint?.TotalLaps ??
                    stint?.LapCount ??
                    stint?.laps ??
                    null,

                stint_number:
                    stint?.Stint ??
                    stint?.StintNumber ??
                    stint?.number ??
                    null,

                /*
                   IMPORTANTE:
                   Boolean("false") === true en JavaScript.
                   Por eso antes todos podían aparecer como PIT.
                */
                in_pit:
                    normalizeLiveBoolean(
                        line.InPit ??
                        line.inPit
                    ),

                pit_out:
                    normalizeLiveBoolean(
                        line.PitOut ??
                        line.pitOut
                    ),

                pit_count:
                    Number(
                        line.NumberOfPitStops ??
                        line.PitStopCount ??
                        line.numberOfPitStops ??
                        0
                    ) || 0,

                speed_traps:
                    line.Speeds ??
                    line.speeds ??
                    null
            };
        })
        .filter(Boolean);

    /*
       V1.26.0 · FALLBACK GAP
       Si F1 entrega el intervalo con el coche de delante pero
       no GapToLeader, acumulamos los intervalos por posición.
       No inventa tiempos: sólo suma diferencias oficiales.
    */
    const ordered =
        normalizedRows
            .slice()
            .sort(
                (a,b) =>
                    Number(a.position || 999) -
                    Number(b.position || 999)
            );

    let accumulatedGapSeconds =
        0;

    function parseOfficialGapSeconds(
        value
    ) {
        if (
            value === null ||
            value === undefined
        ) {
            return null;
        }

        const text =
            String(
                value
            )
            .trim();

        if (
            !text ||
            text === "—" ||
            /^(leader|líder)$/i.test(
                text
            )
        ) {
            return 0;
        }

        /*
           Sólo valores de tiempo +x.xxx / x.xxx.
           No intentamos convertir "1 LAP", "2L", etc.
        */
        const match =
            text.match(
                /^\+?(-?\d+(?:\.\d+)?)$/
            );

        if (!match) {
            return null;
        }

        const seconds =
            Number(
                match[1]
            );

        return Number.isFinite(
            seconds
        )
            ? seconds
            : null;
    }

    ordered.forEach(
        (driver, index) => {
            if (index === 0) {
                driver.gap =
                    driver.gap ||
                    "LÍDER";

                driver.interval =
                    driver.interval ||
                    "INTERVAL";

                accumulatedGapSeconds =
                    0;

                return;
            }

            const officialGap =
                parseOfficialGapSeconds(
                    driver.gap
                );

            if (
                Number.isFinite(
                    officialGap
                ) &&
                officialGap > 0
            ) {
                accumulatedGapSeconds =
                    officialGap;

                return;
            }

            const intervalSeconds =
                parseOfficialGapSeconds(
                    driver.interval
                );

            if (
                Number.isFinite(
                    intervalSeconds
                ) &&
                intervalSeconds >= 0
            ) {
                accumulatedGapSeconds +=
                    intervalSeconds;

                driver.gap =
                    "+" +
                    accumulatedGapSeconds
                        .toFixed(3);
            }
        }
    );

    return normalizedRows;
}

function getRaceControlNormalized() {
    const raw =
        topic("RaceControlMessages");

    const messages =
        asArray(
            raw.Messages ||
            raw.messages ||
            raw
        );

    const driverMap =
        getDriverMap();

    return messages
        .filter(
            item =>
                item &&
                typeof item === "object"
        )
        .map(
            item => {
                const number =
                    String(
                        item.RacingNumber ||
                        item.DriverNumber ||
                        item.CarNumber ||
                        ""
                    ).trim();

                const driver =
                    number
                        ? driverMap[number]
                        : null;

                const details = {};

                Object.entries(
                    item
                ).forEach(
                    ([key, value]) => {
                        if (
                            key === "Message" ||
                            key === "Text"
                        ) {
                            return;
                        }

                        if (
                            value === null ||
                            value === undefined ||
                            value === ""
                        ) {
                            return;
                        }

                        if (
                            typeof value === "string" ||
                            typeof value === "number" ||
                            typeof value === "boolean"
                        ) {
                            details[key] =
                                value;
                            return;
                        }

                        try {
                            details[key] =
                                JSON.stringify(
                                    value
                                );
                        } catch {
                            details[key] =
                                String(
                                    value
                                );
                        }
                    }
                );

                return {
                    time:
                        item.Utc ||
                        item.Timestamp ||
                        item.Time ||
                        null,

                    category:
                        item.Category ||
                        item.Type ||
                        null,

                    flag:
                        item.Flag ||
                        null,

                    scope:
                        item.Scope ||
                        null,

                    status:
                        item.Status ||
                        item.State ||
                        null,

                    mode:
                        item.Mode ||
                        null,

                    sector:
                        item.Sector ||
                        null,

                    racing_number:
                        number ||
                        null,

                    abbreviation:
                        driver?.abbreviation ||
                        null,

                    driver_name:
                        driver?.name ||
                        null,

                    team:
                        driver?.team ||
                        null,

                    team_color:
                        driver?.team_color ||
                        null,

                    source:
                        number
                            ? "DRIVER"
                            : "FIA",

                    lap:
                        item.Lap ||
                        null,

                    message:
                        item.Message ||
                        item.Text ||
                        "",

                    /*
                       V1.25.0
                       Conservamos todos los campos publicados por
                       RaceControlMessages para que el frontend no
                       pierda información aunque F1 añada campos nuevos.
                    */
                    details
                };
            }
        );
}

function getPenaltiesNormalized(raceControl) {
    const penaltyRegex =
        /(penalty|investigation|investigated|noted|black and white|warning|reprimand|drive through|stop go|stop-and-go|time penalty|grid penalty|disqualified|disqualification|fine|no further action|under investigation)/i;

    return raceControl.filter(item =>
        penaltyRegex.test(
            String(item.message || "")
        )
    );
}

function getTrackLimitsNormalized(raceControl) {
    const regex =
        /(track limit|track limits|lap time deleted|lap time deletion|deleted lap|time deleted)/i;

    return raceControl.filter(item =>
        regex.test(
            String(item.message || "")
        )
    );
}

function buildTeamRadioStaticRelativePath(
    relativePath,
    archivePath
) {
    if (!relativePath) {
        return null;
    }

    let cleanFile =
        String(
            relativePath
        ).trim();

    if (!cleanFile) {
        return null;
    }

    if (/^https?:\/\//i.test(cleanFile)) {
        try {
            const url =
                new URL(
                    cleanFile
                );

            if (
                url.hostname !==
                "livetiming.formula1.com"
            ) {
                return null;
            }

            cleanFile =
                url.pathname
                    .replace(
                        /^\/static\//i,
                        ""
                    )
                    .replace(
                        /^\/+/,
                        ""
                    );
        } catch {
            return null;
        }
    } else {
        cleanFile =
            cleanFile.replace(
                /^\/+/,
                ""
            );
    }

    const cleanBase =
        String(
            archivePath ||
            ""
        )
        .replace(
            /^\/+/,
            ""
        )
        .replace(
            /\/+$/,
            ""
        );

    if (
        cleanBase &&
        (
            cleanFile === cleanBase ||
            cleanFile.startsWith(
                cleanBase + "/"
            )
        )
    ) {
        return cleanFile;
    }

    return cleanBase
        ? (
            cleanBase +
            "/" +
            cleanFile
        )
        : cleanFile;
}

function getTeamRadioNormalized() {
    const raw = topic("TeamRadio");
    const captures = asArray(
        raw.Captures ||
        raw.captures ||
        raw
    );

    const driverMap = getDriverMap();

    const archivePath =
        topic("SessionInfo")?.Path ||
        topic("SessionInfo")?.path ||
        "";

    return captures
        .filter(
            item =>
                item &&
                typeof item === "object"
        )
        .map(
            item => {
                const number =
                    String(
                        item.RacingNumber ||
                        item.DriverNumber ||
                        ""
                    );

                const relativePath =
                    item.Path ||
                    item.path ||
                    null;

                const staticRelativePath =
                    buildTeamRadioStaticRelativePath(
                        relativePath,
                        archivePath
                    );

                let directUrl = null;

                if (staticRelativePath) {
                    directUrl =
                        F1_STATIC_BASE +
                        "/" +
                        staticRelativePath;
                }

                const proxyPath =
                    staticRelativePath
                        ? (
                            "/api/live/team-radio/audio?path=" +
                            encodeURIComponent(
                                staticRelativePath
                            )
                        )
                        : null;

                return {
                    time:
                        item.Utc ||
                        item.Timestamp ||
                        null,

                    driver_number:
                        number,

                    abbreviation:
                        driverMap[number]?.abbreviation ||
                        number,

                    name:
                        driverMap[number]?.name ||
                        number,

                    team:
                        driverMap[number]?.team ||
                        "",

                    team_color:
                        driverMap[number]?.team_color ||
                        null,

                    audio_url:
                        directUrl,

                    audio_proxy_path:
                        proxyPath,

                    static_relative_path:
                        staticRelativePath,

                    message:
                        number
                            ? (
                                "Team Radio · " +
                                (
                                    driverMap[number]?.abbreviation ||
                                    number
                                )
                            )
                            : "Team Radio"
                };
            }
        );
}

function getWeatherNormalized() {
    const weather = topic("WeatherData");

    return {
        air_temperature:
            weather.AirTemp ??
            weather.AirTemperature ??
            weather.air_temperature ??
            null,
        track_temperature:
            weather.TrackTemp ??
            weather.TrackTemperature ??
            weather.track_temperature ??
            null,
        humidity:
            weather.Humidity ??
            weather.humidity ??
            null,
        pressure:
            weather.Pressure ??
            weather.pressure ??
            null,
        rainfall:
            weather.Rainfall ??
            weather.rainfall ??
            null,
        wind_speed:
            weather.WindSpeed ??
            weather.wind_speed ??
            null,
        wind_direction:
            weather.WindDirection ??
            weather.wind_direction ??
            null
    };
}

function getSessionStatusText() {
    const raw = topic("SessionStatus");

    return (
        raw.Status ||
        raw.status ||
        raw.SessionStatus ||
        null
    );
}

function getSessionLifecycle() {
    const status =
        String(
            getSessionStatusText() ||
            ""
        )
            .trim()
            .toLowerCase();

    const sessionInfo = topic("SessionInfo");
    const lapCount = topic("LapCount");
    const clock = topic("ExtrapolatedClock");
    const timingData = topic("TimingDataF1");
    const timingClassic = topic("TimingData");

    const currentLap =
        Number(
            lapCount.CurrentLap ??
            lapCount.currentLap
        );

    const totalLaps =
        Number(
            lapCount.TotalLaps ??
            lapCount.totalLaps
        );

    const endDate =
        sessionInfo.EndDate ||
        sessionInfo.endDate ||
        null;

    const finishedWords = [
        "finished",
        "finalised",
        "finalized",
        "ended",
        "complete",
        "completed",
        "inactive",
        "off"
    ];

    const liveWords = [
        "started",
        "live",
        "active",
        "green"
    ];

    const remainingClock =
        String(
            clock.Remaining ??
            clock.remaining ??
            ""
        )
        .trim();

    const clockFinished =
        /^(?:0+:)?00:00(?:\.000)?$/.test(
            remainingClock
        ) ||
        remainingClock === "00:00:00" ||
        remainingClock === "00:00";

    const timingLines =
        timingData.Lines ||
        timingData.lines ||
        timingClassic.Lines ||
        timingClassic.lines ||
        {};

    const hasClassification =
        Object.values(
            timingLines
        )
        .some(
            line =>
                line &&
                (
                    line.Position ||
                    line.position ||
                    line.BestLapTime ||
                    line.bestLapTime ||
                    line.LastLapTime ||
                    line.lastLapTime
                )
        );

    let finished =
        finishedWords.some(
            word => status.includes(word)
        );

    if (
        !finished &&
        Number.isFinite(currentLap) &&
        Number.isFinite(totalLaps) &&
        totalLaps > 0 &&
        currentLap >= totalLaps
    ) {
        finished = true;
    }

    if (
        !finished &&
        endDate
    ) {
        const endTime =
            new Date(endDate).getTime();

        if (
            Number.isFinite(endTime) &&
            Date.now() > endTime
        ) {
            finished = true;
        }
    }

    if (
        !finished &&
        clockFinished &&
        hasClassification
    ) {
        finished = true;
    }

    const live =
        !finished &&
        state.connected &&
        liveWords.some(
            word => status.includes(word)
        );

    return {
        live,
        finished,
        label:
            finished
                ? "SESIÓN FINALIZADA"
                : live
                    ? "EN DIRECTO"
                    : state.connected
                        ? "CONECTADO"
                        : "SIN CONEXIÓN"
    };
}


let liveQualifyingPhaseLock = null;
let liveQualifyingSessionIdentity = null;

function getLiveQualifyingPhase() {
    const sessionInfo =
        topic("SessionInfo") ||
        {};

    const sessionName =
        String(
            sessionInfo.Name ||
            sessionInfo.SessionName ||
            ""
        )
        .trim();

    const isSprintQualifying =
        /sprint qualifying|sprint shootout/i.test(
            sessionName
        );

    const isQualifying =
        !isSprintQualifying &&
        /qualifying/i.test(
            sessionName
        );

    if (
        !isQualifying &&
        !isSprintQualifying
    ) {
        return null;
    }

    /*
       SessionData publica una serie de estados.
       En Qualifying hay un STARTED por cada fase.
       1er STARTED = Q1 / SQ1
       2do STARTED = Q2 / SQ2
       3er STARTED = Q3 / SQ3
    */
    const sessionData =
        topic("SessionData") ||
        {};

    const statusSeries =
        asArray(
            sessionData.StatusSeries ??
            sessionData.statusSeries ??
            []
        );

    let startedCount =
        0;

    statusSeries.forEach(
        entry => {
            const status =
                String(
                    entry?.SessionStatus ??
                    entry?.sessionStatus ??
                    entry?.Status ??
                    entry?.status ??
                    ""
                )
                .trim()
                .toLowerCase();

            if (
                status.includes(
                    "started"
                )
            ) {
                startedCount += 1;
            }
        }
    );

    /*
       Si todavía no llegó SessionData pero la sesión está activa,
       asumimos la primera fase. En cuanto llegue la serie oficial,
       se corrige automáticamente.
    */
    const part =
        Math.max(
            1,
            Math.min(
                3,
                startedCount || 1
            )
        );

    const identity =
        String(
            sessionInfo.Path ||
            sessionInfo.path ||
            [
                sessionInfo?.Meeting?.Name || "",
                sessionName
            ].join("|")
        );

    if (liveQualifyingSessionIdentity !== identity) {
        liveQualifyingSessionIdentity = identity;
        liveQualifyingPhaseLock = null;
    }

    const candidate =
        (isSprintQualifying ? "SQ" : "Q") + part;

    const candidatePart =
        Number(candidate.replace(/\D/g,"")) || 1;

    const lockedPart =
        Number(String(liveQualifyingPhaseLock || "").replace(/\D/g,"")) || 0;

    if (!liveQualifyingPhaseLock || candidatePart >= lockedPart) {
        liveQualifyingPhaseLock = candidate;
    }

    return liveQualifyingPhaseLock;
}


const liveDriverDisplayHold = new Map();

function holdLiveDriverDisplayFields(driver) {
    const number =
        String(driver?.driver_number || driver?.racing_number || "").trim();

    if (!number) return driver;

    const previous =
        liveDriverDisplayHold.get(number) || {};

    const merged = {...driver};

    const fields = [
        "interval",
        "gap_to_leader",
        "tyre",
        "compound",
        "tyre_laps",
        "stint",
        "best_lap",
        "last_lap",
        "sectors",
        "best_sector_1",
        "best_sector_2",
        "best_sector_3"
    ];

    const meaningful = value =>
        value !== null &&
        value !== undefined &&
        String(value).trim() !== "" &&
        String(value).trim() !== "—";

    for (const field of fields) {
        if (meaningful(driver?.[field])) {
            previous[field] = driver[field];
        } else if (meaningful(previous?.[field])) {
            merged[field] = previous[field];
        }
    }

    liveDriverDisplayHold.set(number,previous);
    return merged;
}

function buildNormalizedSnapshot() {
    const sessionInfo = topic("SessionInfo");
    const meeting = sessionInfo.Meeting || {};
    const lapCount = topic("LapCount");
    const track = topic("TrackStatus");
    const clock = topic("ExtrapolatedClock");
    const raceControl = getRaceControlNormalized();
    const lifecycle = getSessionLifecycle();
    const positions = getDriverPositionsNormalized();

    return {
        success: true,
        source: "f1_livetiming",
        connected: state.connected,
        connection_phase: state.phase,
        generated_at: new Date().toISOString(),
        last_message_at: state.last_message_at,

        meeting_name:
            meeting.Name ||
            meeting.OfficialName ||
            sessionInfo.MeetingName ||
            null,

        circuit_name:
            meeting.Circuit?.ShortName ||
            meeting.Circuit?.Name ||
            meeting.Location ||
            null,

        country_name:
            meeting.Country?.Name ||
            meeting.Country?.OfficialName ||
            null,

        country_code:
            meeting.Country?.Code ||
            meeting.Country?.Key ||
            null,

        session_name:
            sessionInfo.Name ||
            sessionInfo.SessionName ||
            null,

        session_phase:
            getLiveQualifyingPhase(),

        session_status:
            getSessionStatusText(),

        is_live:
            lifecycle.live,

        is_finished:
            lifecycle.finished,

        live_label:
            lifecycle.label,

        track_status:
            track.Message ||
            track.Status ||
            track.status ||
            null,

        clock:
            clock.Remaining ||
            clock.remaining ||
            null,

        clock_utc:
            clock.Utc ||
            clock.utc ||
            null,

        clock_extrapolating:
            normalizeLiveBoolean(
                clock.Extrapolating ??
                clock.extrapolating ??
                lifecycle.live
            ),

        lap:
            lapCount.CurrentLap ??
            lapCount.currentLap ??
            null,

        total_laps:
            lapCount.TotalLaps ??
            lapCount.totalLaps ??
            null,

        archive_path:
            sessionInfo.Path ||
            sessionInfo.path ||
            null,

        weather:
            getWeatherNormalized(),

        drivers:
            getDriversNormalized()
                .map(holdLiveDriverDisplayFields),

        positions,

        position_count:
            Object.keys(positions).length,

        live_track_points:
            Array.from(
                liveTrackTrajectories.values()
            ).reduce(
                (max, points) =>
                    Math.max(max, points.length),
                0
            ),

        race_control:
            raceControl,

        penalties:
            getPenaltiesNormalized(raceControl),

        track_limits:
            getTrackLimitsNormalized(raceControl),

        team_radio:
            getTeamRadioNormalized(),

        /*
           V1.14.0 · SESSION DATA HUB
           Exponemos también los bloques oficiales que ya mantiene
           este servicio en memoria. El frontend puede construir
           vistas avanzadas de FP1/FP2/FP3/Qualy/Race sin conectarse
           directamente a F1.
        */
        session_data:
            topic("SessionData"),

        timing_app_data:
            topic("TimingAppData"),

        timing_stats:
            topic("TimingStats"),

        top_three:
            topic("TopThree"),

        car_data:
            topic("CarData"),

        extrapolated_clock:
            topic("ExtrapolatedClock"),

        pit_lane_time_collection:
            topic("PitLaneTimeCollection"),

        tyre_stint_series:
            topic("TyreStintSeries"),

        driver_race_info:
            topic("DriverRaceInfo")
    };
}


/* =========================================================
   SIGNALR CORE
========================================================= */

async function negotiateSignalR() {
    const response = await fetch(
        `https://${F1_HOST}${SIGNALR_PATH}/negotiate?negotiateVersion=1`,
        {
            method: "POST",
            headers: {
                "User-Agent": "BestHTTP",
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, identity",
                "Content-Type": "application/json",
                "Origin": "https://www.formula1.com"
            },
            body: "{}"
        }
    );

    const text = await response.text();

    if (!response.ok) {
        throw new Error(
            `SignalR negotiate HTTP ${response.status}: ${text.slice(0, 300)}`
        );
    }

    const data = JSON.parse(text);

    const cookies = typeof response.headers.getSetCookie === "function"
        ? response.headers
            .getSetCookie()
            .map(cookie => cookie.split(";")[0])
            .join("; ")
        : "";

    return {
        ...data,
        cookies
    };
}

function sendSignalRFrame(payload) {
    if (
        !f1Socket ||
        f1Socket.readyState !== WebSocket.OPEN
    ) {
        return;
    }

    f1Socket.send(
        JSON.stringify(payload) +
        RECORD_SEPARATOR
    );
}

function handleSignalRFeed(topicName, rawData) {
    applyTopicUpdate(
        topicName,
        rawData
    );
}

function handleSignalRFrames(rawText) {
    const frames = String(rawText)
        .split(RECORD_SEPARATOR)
        .filter(frame => frame.trim());

    for (const frame of frames) {
        let message;

        try {
            message = JSON.parse(frame);
        } catch {
            continue;
        }

        if (
            message.type === 1 &&
            message.target === "feed" &&
            Array.isArray(message.arguments) &&
            message.arguments.length >= 2
        ) {
            handleSignalRFeed(
                message.arguments[0],
                message.arguments[1]
            );
            continue;
        }

        if (
            message.type === 3 &&
            message.invocationId === "0" &&
            message.result &&
            typeof message.result === "object"
        ) {
            for (
                const [topicName, topicData]
                of Object.entries(message.result)
            ) {
                if (
                    topicData !== null &&
                    topicData !== undefined
                ) {
                    handleSignalRFeed(
                        topicName,
                        topicData
                    );
                }
            }

            continue;
        }

        if (message.type === 6) {
            sendSignalRFrame({ type: 6 });
            continue;
        }

        if (message.type === 7) {
            state.last_error =
                message.error ||
                "F1 solicitó cerrar la conexión.";
        }
    }
}


function markF1SocketActivity() {
    lastSocketActivityAt =
        Date.now();
}


function clearConnectionWatchdog() {
    if (connectionWatchdogTimer) {
        clearInterval(
            connectionWatchdogTimer
        );

        connectionWatchdogTimer =
            null;
    }
}


function startConnectionWatchdog() {
    clearConnectionWatchdog();

    markF1SocketActivity();

    connectionWatchdogTimer =
        setInterval(
            () => {
                if (
                    stopped ||
                    !state.connected ||
                    !f1Socket
                ) {
                    return;
                }

                const age =
                    Date.now() -
                    lastSocketActivityAt;

                if (
                    age <
                    F1_SOCKET_STALE_MS
                ) {
                    return;
                }

                /*
                   El WebSocket puede quedar "abierto" para Node aunque
                   F1 haya dejado de enviar datos. En ese estado no se
                   dispara close/error y el servicio parece conectado,
                   pero el frontend deja de actualizar.

                   Forzamos el cierre y dejamos que scheduleReconnect()
                   negocie una conexión nueva.
                */
                state.last_error =
                    `F1 Live Timing sin actividad durante ${Math.round(age / 1000)} s. Reconectando.`;

                state.phase =
                    "stale_connection";

                console.warn(
                    "[F1] Stale connection detected. Forcing reconnect..."
                );

                try {
                    f1Socket.terminate();
                } catch (error) {
                    console.warn(
                        "[F1] Could not terminate stale socket:",
                        error.message
                    );

                    state.connected =
                        false;

                    clearKeepAlive();

                    scheduleReconnect();
                }
            },
            5000
        );
}


function clearKeepAlive() {
    if (keepAliveTimer) {
        clearInterval(keepAliveTimer);
        keepAliveTimer = null;
    }
}

function scheduleReconnect() {
    if (
        stopped ||
        reconnectTimer
    ) {
        return;
    }

    clearConnectionWatchdog();

    state.reconnect_attempt += 1;
    state.phase = "waiting_reconnect";

    const delay = reconnectDelayMs;

    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;

        reconnectDelayMs =
            Math.min(
                reconnectDelayMs * 2,
                30000
            );

        connectToF1();
    }, delay);
}

async function connectToF1() {
    if (stopped) {
        return;
    }

    state.phase = "negotiating";
    state.last_error = null;

    let negotiation;

    try {
        negotiation = await negotiateSignalR();
    } catch (error) {
        state.connected = false;
        state.phase = "negotiate_failed";
        state.last_error = error.message;

        console.error(
            "[F1] Negotiate failed:",
            error.message
        );

        scheduleReconnect();
        return;
    }

    const token =
        negotiation.connectionToken ||
        negotiation.connectionId;

    if (!token) {
        state.last_error =
            "SignalR no devolvió connectionToken/connectionId.";
        scheduleReconnect();
        return;
    }

    const wsUrl =
        `wss://${F1_HOST}${SIGNALR_PATH}?id=${encodeURIComponent(token)}`;

    const headers = {
        "User-Agent": "BestHTTP",
        "Origin": "https://www.formula1.com"
    };

    if (negotiation.cookies) {
        headers.Cookie =
            negotiation.cookies;
    }

    let handshakeComplete = false;

    f1Socket =
        new WebSocket(
            wsUrl,
            { headers }
        );

    f1Socket.on("open", () => {
        state.phase =
            "websocket_connected";

        reconnectDelayMs =
            2000;

        sendSignalRFrame({
            protocol: "json",
            version: 1
        });
    });

    f1Socket.on("message", buffer => {
        markF1SocketActivity();

        const raw =
            buffer.toString();

        if (!handshakeComplete) {
            const frames =
                raw
                    .split(RECORD_SEPARATOR)
                    .filter(frame => frame.trim());

            let handshake = {};

            if (frames.length) {
                try {
                    handshake =
                        JSON.parse(
                            frames[0]
                        );
                } catch {
                    handshake = {};
                }
            }

            if (handshake.error) {
                state.connected = false;
                state.phase =
                    "handshake_failed";
                state.last_error =
                    handshake.error;

                f1Socket.terminate();
                return;
            }

            handshakeComplete = true;
            state.connected = true;
            state.phase = "live";
            state.last_connected_at =
                new Date().toISOString();
            state.reconnect_attempt = 0;

            sendSignalRFrame({
                type: 1,
                invocationId: "0",
                target: "Subscribe",
                arguments: [TOPICS]
            });

            clearKeepAlive();

            keepAliveTimer =
                setInterval(
                    () =>
                        sendSignalRFrame({
                            type: 6
                        }),
                    15000
                );

            startConnectionWatchdog();

            if (frames.length > 1) {
                handleSignalRFrames(
                    frames
                        .slice(1)
                        .join(
                            RECORD_SEPARATOR
                        ) +
                    RECORD_SEPARATOR
                );
            }

            events.emit(
                "snapshot",
                buildNormalizedSnapshot()
            );

            console.log(
                `[F1] Connected. Subscribed to ${TOPICS.length} topics.`
            );

            return;
        }

        handleSignalRFrames(raw);
    });

    f1Socket.on("close", (code, reason) => {
        state.connected = false;
        state.phase = "disconnected";
        state.last_disconnected_at =
            new Date().toISOString();

        clearKeepAlive();
        clearConnectionWatchdog();

        console.warn(
            `[F1] Disconnected (${code}) ${reason?.toString() || ""}`
        );

        scheduleReconnect();
    });

    f1Socket.on("error", error => {
        state.last_error =
            error.message;

        console.error(
            "[F1] WebSocket:",
            error.message
        );
    });
}


/* =========================================================
   ARCHIVO / REPLAY
========================================================= */

function validateArchivePath(value) {
    const path =
        String(value || "")
            .trim();

    if (
        !path ||
        path.length > 300 ||
        path.includes("..") ||
        path.includes("\\") ||
        !/^[A-Za-z0-9_./-]+$/.test(path)
    ) {
        return null;
    }

    return path.replace(/^\/+/, "");
}

function validateYear(value) {
    const year = Number(value);
    const currentYear =
        new Date().getUTCFullYear();

    if (
        !Number.isInteger(year) ||
        year < 2018 ||
        year > currentYear + 1
    ) {
        return null;
    }

    return year;
}

async function fetchArchiveText(relativePath) {
    const cleanPath =
        validateArchivePath(
            relativePath
        );

    if (!cleanPath) {
        const error =
            new Error(
                "Ruta de archivo inválida."
            );
        error.statusCode = 400;
        throw error;
    }

    const response =
        await fetch(
            `${F1_STATIC_BASE}/${cleanPath}`,
            {
                headers: {
                    "User-Agent":
                        "Chavitoxo-F1-Data-Service/1.0",
                    "Accept":
                        "application/json,text/plain,*/*"
                }
            }
        );

    const text =
        (await response.text())
            .replace(/^\uFEFF/, "");

    if (!response.ok) {
        const error =
            new Error(
                `F1 archive HTTP ${response.status}`
            );
        error.statusCode =
            response.status;
        throw error;
    }

    return {
        text,
        contentType:
            response.headers.get(
                "content-type"
            ) ||
            "text/plain"
    };
}


const archiveTextCache = new Map();
const archiveParsedCache = new Map();
const ARCHIVE_CACHE_MS = 5 * 60 * 1000;

async function fetchArchiveTextCached(relativePath) {
    const cleanPath =
        validateArchivePath(relativePath);

    if (!cleanPath) {
        const error =
            new Error("Ruta de archivo inválida.");
        error.statusCode = 400;
        throw error;
    }

    const cached =
        archiveTextCache.get(cleanPath);

    if (
        cached &&
        Date.now() - cached.loadedAt <
            ARCHIVE_CACHE_MS
    ) {
        return cached.value;
    }

    const value =
        await fetchArchiveText(cleanPath);

    archiveTextCache.set(
        cleanPath,
        {
            loadedAt: Date.now(),
            value
        }
    );

    return value;
}

async function getParsedArchiveStream(
    relativePath,
    compressed
) {
    const cleanPath =
        validateArchivePath(
            relativePath
        );

    if (!cleanPath) {
        const error =
            new Error(
                "Ruta de archivo inválida."
            );

        error.statusCode = 400;
        throw error;
    }

    const cacheKey =
        cleanPath +
        "::" +
        (compressed ? "z" : "plain");

    const cached =
        archiveParsedCache.get(
            cacheKey
        );

    if (
        cached &&
        Date.now() - cached.loadedAt <
            ARCHIVE_CACHE_MS
    ) {
        return cached.records;
    }

    const response =
        await fetchArchiveTextCached(
            cleanPath
        );

    const records =
        parseArchiveJsonStream(
            response.text,
            compressed
        );

    archiveParsedCache.set(
        cacheKey,
        {
            loadedAt:
                Date.now(),
            records
        }
    );

    return records;
}


function parseArchiveOffset(value) {
    const text =
        String(value || "").trim();

    const match =
        text.match(
            /^(\d+):(\d{2}):(\d{2})\.(\d{3})$/
        );

    if (!match) {
        return null;
    }

    return (
        Number(match[1]) * 3600000 +
        Number(match[2]) * 60000 +
        Number(match[3]) * 1000 +
        Number(match[4])
    );
}

function parseCompressedJsonStreamLine(line) {
    const raw =
        String(line || "").trim();

    if (!raw) {
        return null;
    }

    const firstQuote =
        raw.indexOf('"');

    if (firstQuote <= 0) {
        return null;
    }

    const secondQuote =
        raw.indexOf(
            '"',
            firstQuote + 1
        );

    if (secondQuote <= firstQuote + 1) {
        return null;
    }

    const offset =
        raw
            .slice(0, firstQuote)
            .trim();

    const encoded =
        raw.slice(
            firstQuote + 1,
            secondQuote
        );

    try {
        const compressed =
            Buffer.from(
                encoded,
                "base64"
            );

        const inflated =
            zlib.inflateRawSync(
                compressed
            );

        const data =
            JSON.parse(
                inflated.toString("utf8")
            );

        return {
            offset,
            offset_ms:
                parseArchiveOffset(offset),
            data
        };

    } catch {
        return null;
    }
}

function parsePlainJsonStreamLine(line) {
    const raw =
        String(line || "").trim();

    if (!raw) {
        return null;
    }

    /*
       Los jsonStream de F1 pueden traer objetos, arrays
       o strings JSON. Localizamos el primer inicio válido.
    */
    const candidates = [
        raw.indexOf("{"),
        raw.indexOf("["),
        raw.indexOf('"')
    ]
    .filter(
        value =>
            value > 0
    );

    if (!candidates.length) {
        return null;
    }

    const jsonStart =
        Math.min(
            ...candidates
        );

    const offset =
        raw
            .slice(0, jsonStart)
            .trim();

    try {
        const data =
            JSON.parse(
                raw.slice(jsonStart)
            );

        return {
            offset,
            offset_ms:
                parseArchiveOffset(offset),
            data
        };

    } catch {
        return null;
    }
}

function parseArchiveJsonStream(
    text,
    compressed
) {
    const lines =
        String(text || "")
            .split(/\r?\n/);

    const records = [];

    for (const line of lines) {
        const parsed =
            compressed
                ? parseCompressedJsonStreamLine(line)
                : parsePlainJsonStreamLine(line);

        if (parsed) {
            records.push(parsed);
        }
    }

    return records;
}

function extractLatestArchivePositions(
    records,
    allowedDriverNumbers = null
) {
    const latestByDriver =
        Object.create(null);

    const allowed =
        allowedDriverNumbers
            ? new Set(
                Array.from(
                    allowedDriverNumbers,
                    value => String(value)
                )
            )
            : null;

    for (const record of records) {
        const frames =
            asArray(
                record?.data?.Position ||
                record?.data?.position ||
                []
            );

        for (const frame of frames) {
            if (!frame || typeof frame !== "object") {
                continue;
            }

            const entries =
                frame.Entries ||
                frame.entries ||
                {};

            for (
                const [number, car]
                of Object.entries(entries)
            ) {
                const driverNumber =
                    String(number);

                if (
                    allowed &&
                    !allowed.has(driverNumber)
                ) {
                    continue;
                }

                if (
                    !car ||
                    typeof car !== "object"
                ) {
                    continue;
                }

                const x =
                    Number(car.X ?? car.x);
                const y =
                    Number(car.Y ?? car.y);
                const z =
                    Number(car.Z ?? car.z);

                if (
                    !Number.isFinite(x) ||
                    !Number.isFinite(y)
                ) {
                    continue;
                }

                latestByDriver[
                    driverNumber
                ] = {
                    driver_number:
                        driverNumber,
                    x,
                    y,
                    z:
                        Number.isFinite(z)
                            ? z
                            : null,
                    status:
                        car.Status ??
                        car.status ??
                        null,
                    timestamp:
                        frame.Timestamp ||
                        frame.Utc ||
                        frame.timestamp ||
                        null,
                    offset:
                        record.offset,
                    offset_ms:
                        record.offset_ms
                };
            }
        }
    }

    return latestByDriver;
}

function getArchiveDriverNumbers() {
    const driverMap =
        getDriverMap();

    return new Set(
        Object.keys(driverMap)
    );
}

function findArchiveRecordAtOrBefore(
    records,
    targetMs
) {
    if (
        !Array.isArray(records) ||
        !records.length
    ) {
        return null;
    }

    let selected =
        records[0];

    for (const record of records) {
        if (
            !Number.isFinite(
                record?.offset_ms
            )
        ) {
            continue;
        }

        if (
            record.offset_ms <= targetMs
        ) {
            selected = record;
            continue;
        }

        break;
    }

    return selected;
}

function extractPositionsFromArchiveRecord(
    record,
    allowedDriverNumbers = null
) {
    if (!record) {
        return {};
    }

    return extractLatestArchivePositions(
        [record],
        allowedDriverNumbers
    );
}

function getArchiveTopicFile(topicName) {
    const key =
        String(topicName || "")
            .trim()
            .toLowerCase();

    const map = {
        position:
            "Position.z.jsonStream",
        cardata:
            "CarData.z.jsonStream",
        car_data:
            "CarData.z.jsonStream",
        timingdata:
            "TimingData.jsonStream",
        timing_data:
            "TimingData.jsonStream",
        timingdataf1:
            "TimingDataF1.jsonStream",
        timing_data_f1:
            "TimingDataF1.jsonStream",
        timingappdata:
            "TimingAppData.jsonStream",
        timing_app_data:
            "TimingAppData.jsonStream",
        timingstats:
            "TimingStats.jsonStream",
        timing_stats:
            "TimingStats.jsonStream",
        pitlanetimecollection:
            "PitLaneTimeCollection.jsonStream",
        pit_lane_time_collection:
            "PitLaneTimeCollection.jsonStream",
        tyrestintseries:
            "TyreStintSeries.jsonStream",
        tyre_stint_series:
            "TyreStintSeries.jsonStream",
        driverraceinfo:
            "DriverRaceInfo.jsonStream",
        driver_race_info:
            "DriverRaceInfo.jsonStream",
        lapcount:
            "LapCount.jsonStream",
        lap_count:
            "LapCount.jsonStream",
        weather:
            "WeatherData.jsonStream",
        weatherdata:
            "WeatherData.jsonStream",
        racecontrol:
            "RaceControlMessages.json",
        race_control:
            "RaceControlMessages.json",
        teamradio:
            "TeamRadio.jsonStream",
        team_radio:
            "TeamRadio.jsonStream",
        driverlist:
            "DriverList.jsonStream",
        driver_list:
            "DriverList.jsonStream"
    };

    return map[key] || null;
}

function isCompressedArchiveTopicFile(fileName) {
    return /\.z\.jsonStream$/i.test(
        String(fileName || "")
    );
}


/* =========================================================
   API HTTP
========================================================= */

const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use(
    cors({
        origin(origin, callback) {
            if (
                !origin ||
                ALLOWED_ORIGINS.includes(origin)
            ) {
                callback(null, true);
                return;
            }

            callback(
                new Error(
                    "Origen no permitido por CORS."
                )
            );
        },
        methods: ["GET", "OPTIONS"],
        maxAge: 86400
    })
);

app.use(express.json({ limit: "32kb" }));

app.get("/", (req, res) => {
    res.json({
        name:
            "Chavitoxo F1 Data Service",
        version:
            "1.36.0",
        live_source:
            "Formula 1 Live Timing",
        connected:
            state.connected,
        endpoints: [
            "/health",
            "/api/live/status",
            "/api/live/snapshot",
            "/api/live/stream",
            "/api/archive/:year",
            "/api/archive/session-inspect?path=...",
            "/api/archive/track-geometry?path=...",
            "/api/archive/track-svg?path=...",
            "/api/archive/replay-frame?path=...&ms=...",
            "/api/archive/session-analysis?path=...",
            "/api/archive/session-stream?path=...&topic=Position",
            "/api/archive/file?path=..."
        ]
    });
});

app.get("/health", (req, res) => {
    res.json({
        ok: true,
        connected:
            state.connected,
        phase:
            state.phase,
        last_message_at:
            state.last_message_at,
        last_connected_at:
            state.last_connected_at,
        last_disconnected_at:
            state.last_disconnected_at,
        last_error:
            state.last_error,
        reconnect_attempt:
            state.reconnect_attempt,
        socket_activity_age_ms:
            lastSocketActivityAt
                ? Date.now() - lastSocketActivityAt
                : null,
        watchdog_stale_after_ms:
            F1_SOCKET_STALE_MS,
        live_position:
            {
                ...liveTopicDiagnostics,
                cached_drivers:
                    liveLatestPositions.size
            }
    });
});

app.get("/api/live/status", (req, res) => {
    const snapshot =
        buildNormalizedSnapshot();

    res.json({
        success: true,
        connected:
            snapshot.connected,
        phase:
            snapshot.connection_phase,
        meeting_name:
            snapshot.meeting_name,
        session_name:
            snapshot.session_name,
        session_status:
            snapshot.session_status,
        is_live:
            snapshot.is_live,
        is_finished:
            snapshot.is_finished,
        live_label:
            snapshot.live_label,
        track_status:
            snapshot.track_status,
        clock:
            snapshot.clock,
        lap:
            snapshot.lap,
        total_laps:
            snapshot.total_laps,
        last_message_at:
            snapshot.last_message_at
    });
});

app.get("/api/live/snapshot", (req, res) => {
    res.set(
        "Cache-Control",
        "no-store"
    );

    res.json(
        buildNormalizedSnapshot()
    );
});

app.get("/api/live/raw", (req, res) => {
    res.set(
        "Cache-Control",
        "no-store"
    );

    res.json({
        success: true,
        connected:
            state.connected,
        phase:
            state.phase,
        last_message_at:
            state.last_message_at,
        topics:
            state.topics
    });
});

app.get("/api/live/stream", (req, res) => {
    res.status(200);

    res.set({
        "Content-Type":
            "text/event-stream",
        "Cache-Control":
            "no-cache, no-transform",
        "Connection":
            "keep-alive",
        "X-Accel-Buffering":
            "no"
    });

    res.flushHeaders?.();

    const sendSnapshot =
        snapshot => {
            res.write(
                "event: snapshot\n"
            );

            res.write(
                `data: ${JSON.stringify(snapshot)}\n\n`
            );
        };

    const ping =
        setInterval(() => {
            res.write(
                `: ping ${Date.now()}\n\n`
            );
        }, 15000);

    sendSnapshot(
        buildNormalizedSnapshot()
    );

    events.on(
        "snapshot",
        sendSnapshot
    );

    req.on("close", () => {
        clearInterval(ping);

        events.off(
            "snapshot",
            sendSnapshot
        );
    });
});




app.get(
    "/api/live/team-radio/audio",
    async (req, res) => {
        const rawPath =
            String(
                req.query.path ||
                ""
            )
            .trim();

        if (
            !rawPath ||
            rawPath.includes("..") ||
            rawPath.includes("\\") ||
            /^https?:/i.test(rawPath)
        ) {
            return res
                .status(400)
                .json({
                    success:false,
                    message:
                        "Ruta de audio inválida."
                });
        }

        const cleanPath =
            rawPath.replace(
                /^\/+/,
                ""
            );

        const url =
            F1_STATIC_BASE +
            "/" +
            cleanPath;

        try {
            const headers = {
                "User-Agent":
                    "Mozilla/5.0 ChavitoxoF1DataService/1.21",
                "Accept":
                    "audio/mpeg,audio/mp4,audio/*;q=0.9,*/*;q=0.8"
            };

            if (req.headers.range) {
                headers.Range =
                    req.headers.range;
            }

            const upstream =
                await fetch(
                    url,
                    {
                        method:"GET",
                        headers,
                        redirect:"follow"
                    }
                );

            if (!upstream.ok) {
                return res
                    .status(upstream.status)
                    .json({
                        success:false,
                        message:
                            "F1 no devolvió el audio solicitado.",
                        status:
                            upstream.status
                    });
            }

            const contentType =
                upstream.headers.get(
                    "content-type"
                ) ||
                "audio/mpeg";

            const contentLength =
                upstream.headers.get(
                    "content-length"
                );

            const contentRange =
                upstream.headers.get(
                    "content-range"
                );

            const acceptRanges =
                upstream.headers.get(
                    "accept-ranges"
                ) ||
                "bytes";

            res.status(
                upstream.status
            );

            res.set(
                "Content-Type",
                contentType
            );

            res.set(
                "Accept-Ranges",
                acceptRanges
            );

            res.set(
                "Cache-Control",
                "public, max-age=86400"
            );

            if (contentLength) {
                res.set(
                    "Content-Length",
                    contentLength
                );
            }

            if (contentRange) {
                res.set(
                    "Content-Range",
                    contentRange
                );
            }

            const buffer =
                Buffer.from(
                    await upstream.arrayBuffer()
                );

            return res.send(
                buffer
            );

        } catch (error) {
            console.error(
                "Error proxy Team Radio:",
                error
            );

            return res
                .status(502)
                .json({
                    success:false,
                    message:
                        "No se pudo reproducir el Team Radio."
                });
        }
    }
);


app.get("/api/live/track-geometry", (req, res) => {
    const geometry =
        buildLiveTrackGeometrySnapshot();

    res.set(
        "Cache-Control",
        "no-store"
    );

    res.status(
        geometry.success
            ? 200
            : 202
    )
    .json(
        geometry
    );
});


app.get("/api/archive/session-inspect", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        if (!sessionPath) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Ruta de sesión inválida."
                });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        const result = {
            success: true,
            path: basePath,
            position: {
                available: false,
                records: 0,
                latest_positions: {}
            },
            car_data: {
                available: false,
                records: 0,
                latest_entry: null
            }
        };

        try {
            const positionResponse =
                await fetchArchiveTextCached(
                    basePath +
                    "Position.z.jsonStream"
                );

            const positionRecords =
                parseArchiveJsonStream(
                    positionResponse.text,
                    true
                );

            result.position = {
                available:
                    positionRecords.length > 0,
                records:
                    positionRecords.length,
                first_offset:
                    positionRecords[0]?.offset ||
                    null,
                last_offset:
                    positionRecords[
                        positionRecords.length - 1
                    ]?.offset ||
                    null,
                latest_positions:
                    extractLatestArchivePositions(
                        positionRecords,
                        getArchiveDriverNumbers()
                    )
            };

        } catch (error) {
            result.position.error =
                error.message;
        }

        try {
            const carResponse =
                await fetchArchiveTextCached(
                    basePath +
                    "CarData.z.jsonStream"
                );

            const carRecords =
                parseArchiveJsonStream(
                    carResponse.text,
                    true
                );

            result.car_data = {
                available:
                    carRecords.length > 0,
                records:
                    carRecords.length,
                first_offset:
                    carRecords[0]?.offset ||
                    null,
                last_offset:
                    carRecords[
                        carRecords.length - 1
                    ]?.offset ||
                    null,
                latest_entry:
                    carRecords.length
                        ? carRecords[
                            carRecords.length - 1
                        ]
                        : null
            };

        } catch (error) {
            result.car_data.error =
                error.message;
        }

        res.set(
            "Cache-Control",
            "no-store"
        );

        res.json(result);

    } catch (error) {
        res
            .status(
                error.statusCode ||
                500
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});


function collectArchiveDriverTrackPoints(
    records,
    driverNumber
) {
    const result = [];
    const wanted =
        String(driverNumber);

    for (const record of records) {
        const frames =
            asArray(
                record?.data?.Position ||
                record?.data?.position ||
                []
            );

        for (const frame of frames) {
            const car =
                frame?.Entries?.[wanted] ||
                frame?.entries?.[wanted];

            if (!car) {
                continue;
            }

            const x =
                Number(car.X ?? car.x);

            const y =
                Number(car.Y ?? car.y);

            const z =
                Number(car.Z ?? car.z);

            if (
                !Number.isFinite(x) ||
                !Number.isFinite(y) ||
                (x === 0 && y === 0)
            ) {
                continue;
            }

            const timestamp =
                frame.Timestamp ||
                frame.Utc ||
                frame.timestamp ||
                null;

            const timestampMs =
                timestamp
                    ? new Date(
                        timestamp
                    ).getTime()
                    : NaN;

            result.push({
                x,
                y,
                z:
                    Number.isFinite(z)
                        ? z
                        : null,
                status:
                    car.Status ??
                    car.status ??
                    null,
                timestamp,
                timestamp_ms:
                    Number.isFinite(
                        timestampMs
                    )
                        ? timestampMs
                        : null,
                offset_ms:
                    Number(
                        record.offset_ms
                    )
            });
        }
    }

    return result;
}


function chooseArchiveTrackDriver(
    records,
    allowedDriverNumbers
) {
    let bestDriver = null;
    let bestScore = -1;

    for (const number of allowedDriverNumbers) {
        const points =
            collectArchiveDriverTrackPoints(
                records,
                number
            );

        if (points.length < 50) {
            continue;
        }

        let movingDistance = 0;

        for (
            let index = 1;
            index < points.length;
            index++
        ) {
            const previous =
                points[index - 1];

            const current =
                points[index];

            const step =
                Math.hypot(
                    current.x -
                        previous.x,
                    current.y -
                        previous.y
                );

            if (
                step > 1 &&
                step < 500
            ) {
                movingDistance +=
                    step;
            }
        }

        const score =
            movingDistance +
            points.length * 10;

        if (score > bestScore) {
            bestScore = score;
            bestDriver =
                String(number);
        }
    }

    return bestDriver;
}


function getPointTimeMs(point) {
    const timestamp =
        Number(
            point?.timestamp_ms
        );

    if (Number.isFinite(timestamp)) {
        return timestamp;
    }

    const offset =
        Number(
            point?.offset_ms
        );

    return Number.isFinite(offset)
        ? offset
        : null;
}


function getMovementDistance(
    points,
    startIndex,
    endIndex
) {
    let distance = 0;

    for (
        let index =
            Math.max(
                1,
                startIndex + 1
            );
        index <= endIndex &&
        index < points.length;
        index++
    ) {
        const previous =
            points[index - 1];

        const current =
            points[index];

        const step =
            Math.hypot(
                current.x -
                    previous.x,
                current.y -
                    previous.y
            );

        if (
            step > 0.5 &&
            step < 500
        ) {
            distance += step;
        }
    }

    return distance;
}


function findSustainedMovementStart(
    points
) {
    if (
        !Array.isArray(points) ||
        points.length < 20
    ) {
        return 0;
    }

    const WINDOW_MS =
        12000;

    const MIN_DISTANCE =
        700;

    let endIndex = 1;

    for (
        let startIndex = 0;
        startIndex <
            points.length - 2;
        startIndex++
    ) {
        const startTime =
            getPointTimeMs(
                points[startIndex]
            );

        if (
            !Number.isFinite(
                startTime
            )
        ) {
            continue;
        }

        if (endIndex <= startIndex) {
            endIndex =
                startIndex + 1;
        }

        while (
            endIndex <
                points.length &&
            Number.isFinite(
                getPointTimeMs(
                    points[endIndex]
                )
            ) &&
            getPointTimeMs(
                points[endIndex]
            ) -
                startTime <
                WINDOW_MS
        ) {
            endIndex++;
        }

        if (
            endIndex >=
                points.length
        ) {
            break;
        }

        const distance =
            getMovementDistance(
                points,
                startIndex,
                endIndex
            );

        if (
            distance >=
                MIN_DISTANCE
        ) {
            return startIndex;
        }
    }

    return 0;
}


function findSustainedMovementEnd(
    points,
    startIndex
) {
    if (
        !Array.isArray(points) ||
        points.length < 20
    ) {
        return (
            points.length - 1
        );
    }

    const WINDOW_MS =
        12000;

    const MIN_DISTANCE =
        500;

    for (
        let endIndex =
            points.length - 1;
        endIndex >
            Math.max(
                startIndex + 2,
                2
            );
        endIndex--
    ) {
        const endTime =
            getPointTimeMs(
                points[endIndex]
            );

        if (
            !Number.isFinite(
                endTime
            )
        ) {
            continue;
        }

        let beginIndex =
            endIndex - 1;

        while (
            beginIndex >
                startIndex &&
            Number.isFinite(
                getPointTimeMs(
                    points[beginIndex]
                )
            ) &&
            endTime -
                getPointTimeMs(
                    points[beginIndex]
                ) <
                WINDOW_MS
        ) {
            beginIndex--;
        }

        const distance =
            getMovementDistance(
                points,
                beginIndex,
                endIndex
            );

        if (
            distance >=
                MIN_DISTANCE
        ) {
            return endIndex;
        }
    }

    return points.length - 1;
}


function buildRealSingleLapGeometry(
    points,
    activeStartIndex
) {
    if (
        !Array.isArray(points) ||
        points.length < 30
    ) {
        return [];
    }

    const startIndex =
        Math.max(
            0,
            Math.min(
                points.length - 1,
                Number(
                    activeStartIndex
                ) || 0
            )
        );

    /*
       No arrancamos exactamente en el primer
       instante de movimiento. Damos unos
       segundos para salir de parrilla/pit y
       elegir un punto claramente en pista.
    */
    let candidateIndex =
        startIndex;

    const startTime =
        getPointTimeMs(
            points[startIndex]
        );

    if (
        Number.isFinite(
            startTime
        )
    ) {
        for (
            let index =
                startIndex;
            index <
                points.length;
            index++
        ) {
            const time =
                getPointTimeMs(
                    points[index]
                );

            if (
                Number.isFinite(time) &&
                time -
                    startTime >=
                    15000
            ) {
                candidateIndex =
                    index;
                break;
            }
        }
    }

    const candidate =
        points[candidateIndex];

    let previous =
        candidate;

    let travelled =
        0;

    const lap = [
        candidate
    ];

    const candidateTime =
        getPointTimeMs(
            candidate
        );

    for (
        let index =
            candidateIndex + 1;
        index <
            points.length;
        index++
    ) {
        const point =
            points[index];

        const pointTime =
            getPointTimeMs(
                point
            );

        if (
            !Number.isFinite(
                pointTime
            ) ||
            !Number.isFinite(
                candidateTime
            )
        ) {
            continue;
        }

        const elapsed =
            pointTime -
            candidateTime;

        if (
            elapsed >
                180000
        ) {
            break;
        }

        const step =
            Math.hypot(
                point.x -
                    previous.x,
                point.y -
                    previous.y
            );

        if (
            step > 0.5 &&
            step < 500
        ) {
            travelled +=
                step;

            lap.push(
                point
            );

            previous =
                point;
        }

        const backToStart =
            Math.hypot(
                point.x -
                    candidate.x,
                point.y -
                    candidate.y
            );

        /*
           Una vuelta de F1 no puede ser
           20-30 segundos. Exigimos un tiempo
           y distancia razonables antes de
           aceptar el cierre.
        */
        if (
            elapsed >=
                50000 &&
            travelled >=
                3000 &&
            backToStart <=
                120
        ) {
            return lap;
        }
    }

    /*
       Segundo intento: buscamos cualquier
       cierre válido dentro del tramo activo.
    */
    const searchEnd =
        Math.min(
            points.length - 1,
            candidateIndex + 5000
        );

    for (
        let originIndex =
            candidateIndex;
        originIndex <
            searchEnd - 100;
        originIndex += 25
    ) {
        const origin =
            points[originIndex];

        const originTime =
            getPointTimeMs(
                origin
            );

        if (
            !Number.isFinite(
                originTime
            )
        ) {
            continue;
        }

        let distance =
            0;

        let last =
            origin;

        const candidateLap = [
            origin
        ];

        for (
            let index =
                originIndex + 1;
            index <=
                searchEnd;
            index++
        ) {
            const point =
                points[index];

            const time =
                getPointTimeMs(
                    point
                );

            if (
                !Number.isFinite(time)
            ) {
                continue;
            }

            const elapsed =
                time -
                originTime;

            if (
                elapsed >
                    180000
            ) {
                break;
            }

            const step =
                Math.hypot(
                    point.x -
                        last.x,
                    point.y -
                        last.y
                );

            if (
                step > 0.5 &&
                step < 500
            ) {
                distance +=
                    step;

                candidateLap.push(
                    point
                );

                last =
                    point;
            }

            const closure =
                Math.hypot(
                    point.x -
                        origin.x,
                    point.y -
                        origin.y
                );

            if (
                elapsed >=
                    50000 &&
                distance >=
                    3000 &&
                closure <=
                    100
            ) {
                return candidateLap;
            }
        }
    }

    return [];
}


function downsampleTrackGeometry(
    points,
    maxPoints = 700
) {
    if (
        !Array.isArray(points)
    ) {
        return [];
    }

    if (
        points.length <=
            maxPoints
    ) {
        return points;
    }

    const step =
        (points.length - 1) /
        (maxPoints - 1);

    const result = [];

    for (
        let index = 0;
        index < maxPoints;
        index++
    ) {
        result.push(
            points[
                Math.min(
                    points.length - 1,
                    Math.round(
                        index * step
                    )
                )
            ]
        );
    }

    return result;
}


function getTrackGeometryBounds(
    points
) {
    if (!points.length) {
        return null;
    }

    const xs =
        points.map(
            point => point.x
        );

    const ys =
        points.map(
            point => point.y
        );

    return {
        min_x:
            Math.min(...xs),
        max_x:
            Math.max(...xs),
        min_y:
            Math.min(...ys),
        max_y:
            Math.max(...ys)
    };
}


function getReplayMovementMeta(
    points
) {
    if (
        !Array.isArray(points) ||
        !points.length
    ) {
        return {
            start_index: 0,
            end_index: 0,
            start_ms: 0,
            end_ms: 0,
            duration_ms: 0
        };
    }

    const startIndex =
        findSustainedMovementStart(
            points
        );

    const endIndex =
        findSustainedMovementEnd(
            points,
            startIndex
        );

    const startMs =
        Number(
            points[startIndex]
                ?.offset_ms
        ) || 0;

    const endMs =
        Number(
            points[endIndex]
                ?.offset_ms
        ) || startMs;

    return {
        start_index:
            startIndex,
        end_index:
            endIndex,
        start_ms:
            startMs,
        end_ms:
            endMs,
        duration_ms:
            Math.max(
                0,
                endMs -
                    startMs
            )
    };
}


function findArchiveRecordAtOrBeforeRelative(
    records,
    replayStartMs,
    relativeMs
) {
    const absoluteTarget =
        Math.max(
            0,
            Number(replayStartMs) +
            Number(relativeMs || 0)
        );

    return {
        absolute_target_ms:
            absoluteTarget,
        record:
            findArchiveRecordAtOrBefore(
                records,
                absoluteTarget
            )
    };
}



function buildTrackSegmentsFromDriverPoints(
    points
) {
    if (
        !Array.isArray(points) ||
        points.length < 2
    ) {
        return [];
    }

    const segments = [];
    let current = [];

    const flush = () => {
        if (current.length >= 2) {
            segments.push(current);
        }

        current = [];
    };

    for (
        let index = 0;
        index < points.length;
        index++
    ) {
        const point =
            points[index];

        if (
            String(
                point?.status || ""
            ).toLowerCase() ===
                "offtrack"
        ) {
            flush();
            continue;
        }

        if (!current.length) {
            current.push(point);
            continue;
        }

        const previous =
            current[
                current.length - 1
            ];

        const step =
            Math.hypot(
                point.x -
                    previous.x,
                point.y -
                    previous.y
            );

        const previousTime =
            getPointTimeMs(
                previous
            );

        const currentTime =
            getPointTimeMs(
                point
            );

        const timeGap =
            (
                Number.isFinite(
                    previousTime
                ) &&
                Number.isFinite(
                    currentTime
                )
            )
                ? currentTime -
                    previousTime
                : 0;

        const broken =
            !Number.isFinite(step) ||
            step > 650 ||
            step === 0 ||
            timeGap < 0 ||
            timeGap > 2500;

        if (broken) {
            flush();
            current.push(point);
            continue;
        }

        current.push(point);
    }

    flush();

    return segments;
}


function simplifyTrackSegment(
    points,
    minDistance = 18,
    maxPoints = 1400
) {
    if (
        !Array.isArray(points) ||
        points.length <= 2
    ) {
        return points || [];
    }

    const simplified = [
        points[0]
    ];

    let lastAccepted =
        points[0];

    for (
        let index = 1;
        index <
            points.length - 1;
        index++
    ) {
        const point =
            points[index];

        const distance =
            Math.hypot(
                point.x -
                    lastAccepted.x,
                point.y -
                    lastAccepted.y
            );

        if (
            distance >=
                minDistance
        ) {
            simplified.push(
                point
            );

            lastAccepted =
                point;
        }
    }

    simplified.push(
        points[
            points.length - 1
        ]
    );

    if (
        simplified.length <=
            maxPoints
    ) {
        return simplified;
    }

    return downsampleTrackGeometry(
        simplified,
        maxPoints
    );
}


function buildTrackCloud(
    segments,
    gridSize = 28
) {
    const unique =
        new Map();

    for (const segment of segments) {
        for (const point of segment) {
            const gx =
                Math.round(
                    Number(point.x) /
                    gridSize
                );

            const gy =
                Math.round(
                    Number(point.y) /
                    gridSize
                );

            const key =
                gx + ":" + gy;

            if (!unique.has(key)) {
                unique.set(
                    key,
                    {
                        x:
                            Number(point.x),
                        y:
                            Number(point.y)
                    }
                );
            }
        }
    }

    return Array.from(
        unique.values()
    );
}


function getBoundsFromSegments(
    segments
) {
    const points = [];

    for (const segment of segments) {
        for (const point of segment) {
            points.push(point);
        }
    }

    return getTrackGeometryBounds(
        points
    );
}


function normalizeSvgPoint(
    point,
    bounds,
    width,
    height,
    padding
) {
    const spanX =
        Math.max(
            1,
            Number(bounds.max_x) -
            Number(bounds.min_x)
        );

    const spanY =
        Math.max(
            1,
            Number(bounds.max_y) -
            Number(bounds.min_y)
        );

    const scale =
        Math.min(
            (width -
                padding * 2) /
                spanX,
            (height -
                padding * 2) /
                spanY
        );

    const drawWidth =
        spanX * scale;

    const drawHeight =
        spanY * scale;

    const offsetX =
        (width -
            drawWidth) / 2;

    const offsetY =
        (height -
            drawHeight) / 2;

    return {
        x:
            offsetX +
            (
                Number(point.x) -
                Number(
                    bounds.min_x
                )
            ) *
            scale,

        y:
            height -
            (
                offsetY +
                (
                    Number(point.y) -
                    Number(
                        bounds.min_y
                    )
                ) *
                scale
            )
    };
}


function renderTrackDebugSvg(
    segments,
    cloud,
    bounds,
    title
) {
    const width = 1200;
    const height = 900;
    const padding = 70;

    const safeTitle =
        String(title || "F1 Track")
            .replace(
                /[&<>"']/g,
                character =>
                    ({
                        "&": "&amp;",
                        "<": "&lt;",
                        ">": "&gt;",
                        '"': "&quot;",
                        "'": "&#39;"
                    })[character]
            );

    const cloudSvg =
        cloud.map(
            point => {
                const p =
                    normalizeSvgPoint(
                        point,
                        bounds,
                        width,
                        height,
                        padding
                    );

                return (
                    `<circle cx="${p.x.toFixed(2)}" ` +
                    `cy="${p.y.toFixed(2)}" ` +
                    `r="2.7" fill="#FFFFFF" opacity="0.58"/>`
                );
            }
        ).join("");

    const segmentSvg =
        segments.map(
            segment => {
                const points =
                    simplifyTrackSegment(
                        segment,
                        16,
                        1200
                    );

                if (points.length < 2) {
                    return "";
                }

                const d =
                    points.map(
                        (point, index) => {
                            const p =
                                normalizeSvgPoint(
                                    point,
                                    bounds,
                                    width,
                                    height,
                                    padding
                                );

                            return (
                                (index === 0
                                    ? "M"
                                    : "L") +
                                p.x.toFixed(2) +
                                " " +
                                p.y.toFixed(2)
                            );
                        }
                    ).join(" ");

                return (
                    `<path d="${d}" fill="none" ` +
                    `stroke="#6D7CFF" stroke-width="2.2" ` +
                    `stroke-linecap="round" stroke-linejoin="round" ` +
                    `opacity="0.28"/>`
                );
            }
        ).join("");

    return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg"
     viewBox="0 0 ${width} ${height}"
     width="${width}"
     height="${height}">
    <rect width="100%" height="100%" fill="#050913"/>
    <text x="40" y="48"
          fill="#FFFFFF"
          font-family="Arial, sans-serif"
          font-size="26"
          font-weight="700">${safeTitle}</text>
    <text x="40" y="78"
          fill="#8D96A8"
          font-family="Arial, sans-serif"
          font-size="15">Chavitoxo F1 Data Service · Track Cloud Debug</text>
    ${segmentSvg}
    ${cloudSvg}
</svg>`;
}



/* =========================================================
   V1.13.0 · MAPA REAL POR SECTORES
========================================================= */

function getTimingDriverLine(record, driverNumber) {
    const data = record?.data || {};
    const lines =
        data.Lines ||
        data.lines ||
        data.TimingData?.Lines ||
        data.TimingData?.lines ||
        data.timingData?.Lines ||
        data.timingData?.lines ||
        data.TimingDataF1?.Lines ||
        data.TimingDataF1?.lines ||
        data.timingDataF1?.Lines ||
        data.timingDataF1?.lines ||
        {};

    return lines?.[String(driverNumber)] || null;
}

function hasMainSectorTimeUpdate(sectorData) {
    if (sectorData === null || sectorData === undefined) return false;

    if (typeof sectorData === "string") {
        return Boolean(sectorData.trim());
    }

    if (typeof sectorData !== "object") return false;

    return [
        sectorData.Value,
        sectorData.value,
        sectorData.PreviousValue,
        sectorData.previousValue
    ].some(
        value =>
            value !== null &&
            value !== undefined &&
            String(value).trim() !== ""
    );
}

function collectTimingSectorEvents(timingRecords, driverNumber) {
    const events = [];

    for (const record of timingRecords) {
        const line = getTimingDriverLine(record, driverNumber);
        if (!line) continue;

        const sectors = line.Sectors || line.sectors;
        if (!sectors) continue;

        for (let index = 0; index < 3; index++) {
            const sector = Array.isArray(sectors)
                ? sectors[index]
                : (sectors[String(index)] ?? sectors[index]);

            if (!hasMainSectorTimeUpdate(sector)) continue;

            const offsetMs = Number(record.offset_ms);
            if (!Number.isFinite(offsetMs)) continue;

            const sameSectorRecent = [...events].reverse().find(
                event => event.sector === index + 1
            );

            if (
                sameSectorRecent &&
                Math.abs(offsetMs - sameSectorRecent.offset_ms) < 1800
            ) {
                continue;
            }

            events.push({
                sector: index + 1,
                offset_ms: offsetMs
            });
        }
    }

    return events.sort((a, b) => a.offset_ms - b.offset_ms);
}

function findBestSectorReferenceLap(driverPoints, timingRecords, driverNumber) {
    const events = collectTimingSectorEvents(timingRecords, driverNumber);
    const finishes = events.filter(event => event.sector === 3);

    if (finishes.length < 2) return null;

    let best = null;

    for (let i = 1; i < finishes.length; i++) {
        const startMs = finishes[i - 1].offset_ms;
        const endMs = finishes[i].offset_ms;
        const duration = endMs - startMs;

        if (duration < 45000 || duration > 210000) continue;

        const s1 = events.find(
            event =>
                event.sector === 1 &&
                event.offset_ms > startMs + 3000 &&
                event.offset_ms < endMs - 3000
        );

        if (!s1) continue;

        const s2 = events.find(
            event =>
                event.sector === 2 &&
                event.offset_ms > s1.offset_ms + 3000 &&
                event.offset_ms < endMs - 2000
        );

        if (!s2) continue;

        const points = driverPoints.filter(point => {
            const t = Number(point.offset_ms);

            return (
                Number.isFinite(t) &&
                t >= startMs &&
                t <= endMs &&
                String(point?.status || "").toLowerCase() !== "offtrack"
            );
        });

        if (points.length < 45) continue;

        const runs = buildTrackSegmentsFromDriverPoints(points);
        const usable = runs.reduce((total, run) => total + run.length, 0);

        if (usable < 40) continue;

        const score = usable - Math.max(0, runs.length - 1) * 70;

        if (!best || score > best.score) {
            best = {
                score,
                start_ms: startMs,
                sector_1_end_ms: s1.offset_ms,
                sector_2_end_ms: s2.offset_ms,
                end_ms: endMs,
                points
            };
        }
    }

    return best;
}

function buildSectorSegmentsFromReferenceLap(referenceLap) {
    if (!referenceLap || !Array.isArray(referenceLap.points)) return [];

    const groups = [
        {
            sector: 1,
            min: referenceLap.start_ms,
            max: referenceLap.sector_1_end_ms
        },
        {
            sector: 2,
            min: referenceLap.sector_1_end_ms,
            max: referenceLap.sector_2_end_ms
        },
        {
            sector: 3,
            min: referenceLap.sector_2_end_ms,
            max: referenceLap.end_ms
        }
    ];

    const output = [];

    for (const group of groups) {
        const points = referenceLap.points.filter(point => {
            const t = Number(point.offset_ms);
            return Number.isFinite(t) && t >= group.min && t <= group.max;
        });

        const runs = buildTrackSegmentsFromDriverPoints(points)
            .filter(run => run.length >= 3);

        for (const run of runs) {
            const simplified = simplifyTrackSegment(run, 14, 900);

            if (simplified.length >= 2) {
                output.push({
                    sector: group.sector,
                    points: simplified
                });
            }
        }
    }

    return output;
}

function getBoundsFromSectorSegments(sectorSegments) {
    const segments = (sectorSegments || [])
        .map(item => item.points)
        .filter(points => Array.isArray(points) && points.length);

    return getBoundsFromSegments(segments);
}


function buildSectorSegmentsFallback(segments) {
    const points = [];

    for (const segment of segments || []) {
        if (!Array.isArray(segment)) continue;

        for (const point of segment) {
            const x = Number(point?.x);
            const y = Number(point?.y);

            if (Number.isFinite(x) && Number.isFinite(y)) {
                points.push({ x, y });
            }
        }
    }

    if (points.length < 12) return [];

    const distances = [0];

    for (let index = 1; index < points.length; index++) {
        distances[index] =
            distances[index - 1] +
            Math.hypot(
                points[index].x - points[index - 1].x,
                points[index].y - points[index - 1].y
            );
    }

    const total =
        distances[distances.length - 1];

    if (!Number.isFinite(total) || total <= 0) {
        return [];
    }

    const groups = [[], [], []];

    for (let index = 0; index < points.length; index++) {
        const ratio =
            distances[index] / total;

        const sector =
            ratio < 1 / 3
                ? 0
                : (
                    ratio < 2 / 3
                        ? 1
                        : 2
                );

        groups[sector].push(points[index]);
    }

    if (groups[0].length && groups[1].length) {
        groups[1].unshift(
            groups[0][groups[0].length - 1]
        );
    }

    if (groups[1].length && groups[2].length) {
        groups[2].unshift(
            groups[1][groups[1].length - 1]
        );
    }

    return groups
        .map((group, index) => ({
            sector: index + 1,
            points: group
        }))
        .filter(item => item.points.length >= 2);
}


app.get("/api/archive/qualifying-phases", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(req.query.path);

        if (!sessionPath) {
            return res.status(400).json({
                success: false,
                message: "Ruta de sesión inválida."
            });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        let positionRecords = [];

        try {
            positionRecords =
                await getParsedArchiveStream(
                    basePath + "Position.z.jsonStream",
                    true
                );
        } catch (error) {
            /*
               Q1/Q2/Q3 can still be resolved from SessionStatus /
               SessionData even when Position.z has not been published.
            */
            console.warn(
                "[F1] Position.z todavía no disponible para fases Replay:",
                error.message
            );
        }

        const phases =
            await getArchiveQualifyingPhases(
                basePath,
                positionRecords
            );

        res.set(
            "Cache-Control",
            "public, max-age=300"
        );

        res.json({
            success: true,
            path: basePath,
            phases
        });

    } catch (error) {
        res.status(
            error.statusCode || 502
        ).json({
            success: false,
            message:
                error.message ||
                "No se pudieron cargar las fases de Qualifying."
        });
    }
});

app.get("/api/archive/track-geometry", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        if (!sessionPath) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Ruta de sesión inválida."
                });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        const records =
            await getParsedArchiveStream(
                basePath +
                "Position.z.jsonStream",
                true
            );

        let timingRecords = [];
        let timingDataRecords = [];
        let timingDataF1Records = [];

        try {
            timingDataRecords =
                await getParsedArchiveStream(
                    basePath +
                    "TimingData.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] TimingData no disponible para sectores:",
                error.message
            );
        }

        try {
            timingDataF1Records =
                await getParsedArchiveStream(
                    basePath +
                    "TimingDataF1.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] TimingDataF1 no disponible para sectores:",
                error.message
            );
        }

        timingRecords = [
            ...timingDataRecords,
            ...timingDataF1Records
        ]
        .sort(
            (a, b) =>
                Number(a?.offset_ms || 0) -
                Number(b?.offset_ms || 0)
        );

        const allowed =
            getArchiveDriverNumbers();

        const requestedDriver =
            String(
                req.query.driver ||
                ""
            ).trim();

        const driverNumber =
            requestedDriver &&
            allowed.has(
                requestedDriver
            )
                ? requestedDriver
                : chooseArchiveTrackDriver(
                    records,
                    allowed
                );

        if (!driverNumber) {
            throw new Error(
                "No se encontró un piloto válido para construir el trazado."
            );
        }

        const driverPoints =
            collectArchiveDriverTrackPoints(
                records,
                driverNumber
            );

        const movement =
            getReplayMovementMeta(
                driverPoints
            );

        const replayWindow =
            await resolveArchiveReplayWindow(
                basePath,
                records,
                movement,
                req.query.phase
            );

        const qualifyingPhases =
            replayWindow
                .qualifying_phases ||
            [];

        /*
           V1.41.0 · La geometría informa dos duraciones:
           - official_duration_ms: reloj reglamentario.
           - playback_duration_ms: hasta que termina la última
             vuelta válida abierta antes de la bandera a cuadros.
        */
        const replayFinalTargetMs =
            await resolveArchiveQualifyingFinalTarget(
                basePath,
                replayWindow,
                req.query.phase
            );

        const playbackDurationMs =
            Math.max(
                Number(
                    replayWindow.duration_ms ||
                    0
                ),
                Number.isFinite(
                    Number(
                        replayFinalTargetMs
                    )
                )
                    ? Math.max(
                        0,
                        Number(
                            replayFinalTargetMs
                        ) -
                        Number(
                            replayWindow.start_ms
                        )
                      )
                    : 0
            );

        const activePoints =
            driverPoints.slice(
                movement.start_index,
                movement.end_index + 1
            );

        const referenceLap =
            timingRecords.length
                ? findBestSectorReferenceLap(
                    driverPoints,
                    timingRecords,
                    driverNumber
                )
                : null;

        const sectorSegments =
            buildSectorSegmentsFromReferenceLap(
                referenceLap
            );

        const rawSegments =
            buildTrackSegmentsFromDriverPoints(
                activePoints
            );

        const segments =
            rawSegments
                .filter(
                    segment =>
                        segment.length >= 5
                )
                .map(
                    segment =>
                        simplifyTrackSegment(
                            segment,
                            18,
                            1400
                        )
                );

        if (!segments.length) {
            throw new Error(
                "No se pudieron construir segmentos válidos del trazado."
            );
        }

        const cloud =
            buildTrackCloud(
                segments,
                28
            );

        const finalSectorSegments =
            sectorSegments.length
                ? sectorSegments
                : buildSectorSegmentsFallback(
                    segments
                );

        const sectorBounds =
            finalSectorSegments.length
                ? getBoundsFromSectorSegments(
                    finalSectorSegments
                )
                : null;

        const bounds =
            sectorBounds ||
            getBoundsFromSegments(
                segments
            );

        res.set(
            "Cache-Control",
            "public, max-age=300"
        );

        res.json({
            success: true,
            path:
                basePath,

            driver_number:
                driverNumber,

            replay_start_ms:
                replayWindow.start_ms,

            replay_end_ms:
                replayWindow.end_ms,

            duration_ms:
                replayWindow.duration_ms,

            official_duration_ms:
                replayWindow.duration_ms,

            playback_duration_ms:
                playbackDurationMs,

            replay_final_target_ms:
                replayFinalTargetMs,

            replay_phase:
                replayWindow.phase,

            replay_phase_source:
                replayWindow.source ||
                null,

            qualifying_phases:
                qualifyingPhases,

            archive_duration_ms:
                records.length
                    ? Number(
                        records[
                            records.length - 1
                        ]?.offset_ms || 0
                    )
                    : 0,

            geometry_mode:
                "track_cloud",

            segment_count:
                segments.length,

            cloud_points:
                cloud.length,

            bounds,

            sector_mode:
                sectorSegments.length
                    ? "timing_data_exact"
                    : (
                        finalSectorSegments.length
                            ? "distance_fallback"
                            : "unavailable"
                    ),

            sector_reference_lap:
                referenceLap
                    ? {
                        start_ms:
                            referenceLap.start_ms,
                        sector_1_end_ms:
                            referenceLap.sector_1_end_ms,
                        sector_2_end_ms:
                            referenceLap.sector_2_end_ms,
                        end_ms:
                            referenceLap.end_ms
                    }
                    : null,

            sector_segments:
                finalSectorSegments.map(
                    item => ({
                        sector:
                            item.sector,
                        points:
                            item.points.map(
                                point => ({
                                    x:
                                        Math.round(
                                            point.x
                                        ),
                                    y:
                                        Math.round(
                                            point.y
                                        )
                                })
                            )
                    })
                ),

            segments:
                segments.map(
                    segment =>
                        segment.map(
                            point => ({
                                x:
                                    Number(point.x),
                                y:
                                    Number(point.y)
                            })
                        )
                ),

            points:
                cloud
        });

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});


app.get("/api/archive/track-svg", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        if (!sessionPath) {
            return res
                .status(400)
                .type("text/plain")
                .send(
                    "Ruta de sesión inválida."
                );
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        const records =
            await getParsedArchiveStream(
                basePath +
                "Position.z.jsonStream",
                true
            );

        const allowed =
            getArchiveDriverNumbers();

        const requestedDriver =
            String(
                req.query.driver ||
                ""
            ).trim();

        const driverNumber =
            requestedDriver &&
            allowed.has(
                requestedDriver
            )
                ? requestedDriver
                : chooseArchiveTrackDriver(
                    records,
                    allowed
                );

        if (!driverNumber) {
            throw new Error(
                "No se encontró piloto válido."
            );
        }

        const driverPoints =
            collectArchiveDriverTrackPoints(
                records,
                driverNumber
            );

        const movement =
            getReplayMovementMeta(
                driverPoints
            );

        const activePoints =
            driverPoints.slice(
                movement.start_index,
                movement.end_index + 1
            );

        const segments =
            buildTrackSegmentsFromDriverPoints(
                activePoints
            )
                .filter(
                    segment =>
                        segment.length >= 5
                );

        const cloud =
            buildTrackCloud(
                segments,
                28
            );

        const bounds =
            getBoundsFromSegments(
                segments
            );

        if (!bounds || !cloud.length) {
            throw new Error(
                "No se pudo construir el mapa."
            );
        }

        const svg =
            renderTrackDebugSvg(
                segments,
                cloud,
                bounds,
                `Driver #${driverNumber} · ${basePath}`
            );

        res.set(
            "Cache-Control",
            "no-store"
        );

        res.type(
            "image/svg+xml"
        );

        res.send(svg);

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .type("text/plain")
            .send(
                error.message
            );
    }
});



/* =========================================================
   V1.13.0 · REPLAY TIMING / DRIVER METADATA
   Reconstruye el estado de TimingData hasta el instante
   solicitado para alimentar la tabla del Replay.
========================================================= */


const archiveSessionDataCache =
    new Map();

async function getArchiveJson(relativePath) {
    const cleanPath =
        validateArchivePath(relativePath);

    if (!cleanPath) {
        const error =
            new Error("Ruta de archivo JSON inválida.");
        error.statusCode = 400;
        throw error;
    }

    const cached =
        archiveSessionDataCache.get(cleanPath);

    if (
        cached &&
        Date.now() - cached.loadedAt < ARCHIVE_CACHE_MS
    ) {
        return cached.value;
    }

    const response =
        await fetchArchiveTextCached(cleanPath);

    let value;

    try {
        value =
            JSON.parse(
                String(response.text || "")
                    .replace(/^\uFEFF/, "")
            );
    } catch (error) {
        const parseError =
            new Error("El archivo JSON de F1 no pudo interpretarse.");
        parseError.statusCode = 502;
        throw parseError;
    }

    archiveSessionDataCache.set(
        cleanPath,
        {
            loadedAt: Date.now(),
            value
        }
    );

    return value;
}

function firstArchivePositionTimestamp(records) {
    for (const record of records || []) {
        const frames =
            Array.isArray(record?.data?.Position)
                ? record.data.Position
                : [];

        for (const frame of frames) {
            const raw =
                frame?.Timestamp ||
                frame?.Utc ||
                null;

            if (!raw) continue;

            const date =
                new Date(raw);

            if (!Number.isNaN(date.getTime())) {
                return {
                    utc_ms: date.getTime(),
                    offset_ms: Number(record?.offset_ms || 0)
                };
            }
        }
    }

    return null;
}

function normalizeStatusSeries(sessionData) {
    const raw =
        sessionData?.StatusSeries ||
        sessionData?.statusSeries ||
        [];

    if (Array.isArray(raw)) {
        return raw.filter(Boolean);
    }

    if (raw && typeof raw === "object") {
        return Object.values(raw).filter(Boolean);
    }

    return [];
}


function getQualifyingPhaseClockMs(
    phaseKey
) {
    const key =
        String(
            phaseKey ||
            ""
        )
        .trim()
        .toUpperCase();

    const clocks = {
        Q1: 18 * 60 * 1000,
        Q2: 15 * 60 * 1000,
        Q3: 12 * 60 * 1000,

        SQ1: 12 * 60 * 1000,
        SQ2: 10 * 60 * 1000,
        SQ3: 8 * 60 * 1000
    };

    return clocks[key] || 0;
}

function buildQualifyingPhaseFallback(
    movement,
    sessionPath
) {
    if (!movement) {
        return [];
    }

    const path =
        String(
            sessionPath ||
            ""
        );

    const sprint =
        /Sprint_Qualifying|Sprint_Shootout/i
            .test(path);

    /*
       Si el archivo no trae transiciones de SessionStatus,
       usamos la estructura reglamentaria del reloj.

       Qualifying:
       Q1 18m · pausa aprox. 7m
       Q2 15m · pausa aprox. 8m
       Q3 12m

       Sprint Qualifying:
       SQ1 12m · pausa aprox. 7m
       SQ2 10m · pausa aprox. 7m
       SQ3 8m

       El objetivo es NO volver a presentar todo el archivo
       de 60+ minutos como si fuera Q1.
    */
    const config =
        sprint
            ? [
                {
                    key: "SQ1",
                    duration: 12 * 60 * 1000,
                    gapAfter: 7 * 60 * 1000
                },
                {
                    key: "SQ2",
                    duration: 10 * 60 * 1000,
                    gapAfter: 7 * 60 * 1000
                },
                {
                    key: "SQ3",
                    duration: 8 * 60 * 1000,
                    gapAfter: 0
                }
            ]
            : [
                {
                    key: "Q1",
                    duration: 18 * 60 * 1000,
                    gapAfter: 7 * 60 * 1000
                },
                {
                    key: "Q2",
                    duration: 15 * 60 * 1000,
                    gapAfter: 8 * 60 * 1000
                },
                {
                    key: "Q3",
                    duration: 12 * 60 * 1000,
                    gapAfter: 0
                }
            ];

    const phases = [];
    let cursor =
        Number(
            movement.start_ms ||
            0
        );

    for (const item of config) {
        const duration =
            item.duration;

        const maxEnd =
            Number(
                movement.end_ms ||
                (
                    cursor +
                    duration
                )
            );

        const end =
            Math.min(
                cursor +
                duration,
                maxEnd
            );

        phases.push({
            key:
                item.key,
            label:
                item.key,
            start_ms:
                cursor,
            end_ms:
                end,
            duration_ms:
                duration,
            source:
                "official_clock_fallback"
        });

        cursor =
            end +
            item.gapAfter;
    }

    return phases;
}

function normalizeArchiveSessionStatus(
    data
) {
    if (
        data === null ||
        data === undefined
    ) {
        return "";
    }

    if (
        typeof data ===
        "string"
    ) {
        return data
            .trim()
            .toLowerCase();
    }

    if (
        typeof data !==
        "object"
    ) {
        return "";
    }

    return String(
        data.Status ??
        data.status ??
        data.SessionStatus ??
        data.sessionStatus ??
        ""
    )
    .trim()
    .toLowerCase();
}

function buildQualifyingPhasesFromStatusRecords(
    statusRecords,
    sessionPath
) {
    const path = String(sessionPath || "");
    const sprint = /Sprint_Qualifying|Sprint_Shootout/i.test(path);
    const prefix = sprint ? "SQ" : "Q";

    const records = (statusRecords || [])
        .map(record => ({
            offset_ms: Number(record?.offset_ms),
            status: normalizeArchiveSessionStatus(record?.data)
        }))
        .filter(item => Number.isFinite(item.offset_ms) && item.status)
        .sort((a,b) => a.offset_ms - b.offset_ms);

    const blocks = [];
    let activeStart = null;

    for (const item of records) {
        if (/started|start|active|green|resumed/.test(item.status)) {
            if (activeStart === null) activeStart = item.offset_ms;
            continue;
        }

        if (
            activeStart !== null &&
            /finished|finish|ended|ends|inactive|suspended|aborted/.test(item.status)
        ) {
            if (item.offset_ms > activeStart) {
                blocks.push({ start_ms: activeStart, end_ms: item.offset_ms });
            }
            activeStart = null;
        }
    }

    if (activeStart !== null) {
        blocks.push({ start_ms: activeStart, end_ms: null });
    }

    /*
       El archivo puede incluir pequeños Started/Finished adicionales.
       Conservamos los tres bloques principales y, sobre todo, el FIN real
       de cada fase. Ese final incluye las vueltas que terminan después de
       que el reloj llega a 00:00.
    */
    const useful = blocks
        .filter((block, index) => {
            if (block.end_ms === null) return true;
            return (block.end_ms - block.start_ms) >= 4 * 60 * 1000;
        });

    const selected = useful.length > 3 ? useful.slice(-3) : useful;
    if (selected.length < 3) return [];

    return selected.slice(0,3).map((block, index) => {
        const key = prefix + String(index + 1);
        const duration = getQualifyingPhaseClockMs(key);
        const realEnd = Number.isFinite(Number(block.end_ms))
            ? Number(block.end_ms)
            : Number(block.start_ms) + duration;

        return {
            key,
            label: key,
            start_ms: Number(block.start_ms),
            end_ms: Math.max(Number(block.start_ms) + duration, realEnd),
            duration_ms: duration,
            source: "session_status_real_end"
        };
    });
}

function buildArchiveQualifyingPhases(
    sessionData,
    positionRecords,
    sessionPath
) {
    const path =
        String(
            sessionPath ||
            ""
        );

    const isSprintQualifying =
        /Sprint_Qualifying|Sprint_Shootout/i
            .test(path);

    const isQualifying =
        isSprintQualifying ||
        /Qualifying/i.test(
            path
        );

    if (!isQualifying) {
        return [];
    }

    const anchor =
        firstArchivePositionTimestamp(
            positionRecords
        );

    if (!anchor) {
        return [];
    }

    const statusEntries =
        normalizeStatusSeries(
            sessionData
        )
        .filter(
            item =>
                item?.SessionStatus &&
                item?.Utc
        )
        .map(
            item => ({
                utc_ms:
                    new Date(
                        item.Utc
                    ).getTime(),
                status:
                    String(
                        item.SessionStatus ||
                        ""
                    )
                    .trim()
                    .toLowerCase()
            })
        )
        .filter(
            item =>
                Number.isFinite(
                    item.utc_ms
                )
        )
        .sort(
            (a, b) =>
                a.utc_ms -
                b.utc_ms
        );

    const starts = [];

    for (
        const item of statusEntries
    ) {
        if (
            /started|start|active|green|resumed/.test(
                item.status
            )
        ) {
            starts.push(
                Math.max(
                    0,
                    Math.round(
                        anchor.offset_ms +
                        (
                            item.utc_ms -
                            anchor.utc_ms
                        )
                    )
                )
            );
        }
    }

    const uniqueStarts = [];

    for (const value of starts) {
        const previous =
            uniqueStarts[
                uniqueStarts.length - 1
            ];

        if (
            previous === undefined ||
            Math.abs(
                value -
                previous
            ) >
            60 * 1000
        ) {
            uniqueStarts.push(
                value
            );
        }
    }

    const selected =
        uniqueStarts.length > 3
            ? uniqueStarts.slice(-3)
            : uniqueStarts;

    if (
        selected.length < 3
    ) {
        return [];
    }

    const prefix =
        isSprintQualifying
            ? "SQ"
            : "Q";

    return selected
        .slice(0, 3)
        .map(
            function (
                startMs,
                index
            ) {
                const key =
                    prefix +
                    String(
                        index + 1
                    );

                const duration =
                    getQualifyingPhaseClockMs(
                        key
                    );

                return {
                    key,
                    label:
                        key,
                    start_ms:
                        startMs,
                    end_ms:
                        startMs +
                        duration,
                    duration_ms:
                        duration,
                    source:
                        "session_data"
                };
            }
        );
}

async function getArchiveQualifyingPhases(
    basePath,
    positionRecords
) {
    let movement = null;

    /*
       El helper anterior espera los puntos de un piloto.
       Si no podemos reutilizarlo, calculamos el rango global
       directamente desde Position.
    */
    const validOffsets =
        (positionRecords || [])
            .map(
                record =>
                    Number(
                        record?.offset_ms
                    )
            )
            .filter(
                Number.isFinite
            );

    if (validOffsets.length) {
        movement = {
            start_ms:
                validOffsets[0],
            end_ms:
                validOffsets[
                    validOffsets.length - 1
                ],
            duration_ms:
                validOffsets[
                    validOffsets.length - 1
                ] -
                validOffsets[0]
        };
    }

    /*
       1) Fuente preferida: SessionStatus.jsonStream.
       Es la forma más directa de localizar el arranque de
       Q1/Q2/Q3 dentro del archivo completo de Qualifying.
    */
    try {
        const statusRecords =
            await getParsedArchiveStream(
                basePath +
                "SessionStatus.jsonStream",
                false
            );

        const fromStatus =
            buildQualifyingPhasesFromStatusRecords(
                statusRecords,
                basePath
            );

        if (
            fromStatus.length === 3
        ) {
            return fromStatus;
        }
    } catch (error) {
        console.warn(
            "[F1] SessionStatus archive no disponible:",
            error.message
        );
    }

    /*
       2) Segundo intento: SessionData.json.
    */
    try {
        const sessionData =
            await getArchiveJson(
                basePath +
                "SessionData.json"
            );

        const fromSessionData =
            buildArchiveQualifyingPhases(
                sessionData,
                positionRecords,
                basePath
            );

        if (
            fromSessionData.length === 3
        ) {
            return fromSessionData;
        }
    } catch (error) {
        console.warn(
            "[F1] SessionData no pudo definir Q1/Q2/Q3:",
            error.message
        );
    }

    /*
       3) Fallback reglamentario.
       Nunca devolvemos el archivo completo como Q1.
    */
    return buildQualifyingPhaseFallback(
        movement,
        basePath
    );
}


function normalizeArchiveSessionStatusValue(data) {
    if (data === null || data === undefined) {
        return "";
    }

    if (typeof data === "string") {
        return data.trim().toLowerCase();
    }

    if (typeof data !== "object") {
        return "";
    }

    return String(
        data.Status ??
        data.status ??
        data.SessionStatus ??
        data.sessionStatus ??
        ""
    )
    .trim()
    .toLowerCase();
}

function chooseArchiveActiveSessionWindow(
    statusRecords,
    movement
) {
    const records =
        (statusRecords || [])
            .map(record => ({
                offset_ms:
                    Number(
                        record?.offset_ms
                    ),

                status:
                    normalizeArchiveSessionStatusValue(
                        record?.data
                    )
            }))
            .filter(
                item =>
                    Number.isFinite(
                        item.offset_ms
                    ) &&
                    item.status
            )
            .sort(
                (a, b) =>
                    a.offset_ms -
                    b.offset_ms
            );

    if (!records.length) {
        return null;
    }

    const starts = [];
    const finishes = [];

    for (const item of records) {
        if (
            /started|start|active|green|resumed/.test(
                item.status
            )
        ) {
            const previous =
                starts[
                    starts.length - 1
                ];

            if (
                previous === undefined ||
                Math.abs(
                    item.offset_ms -
                    previous
                ) >
                30 * 1000
            ) {
                starts.push(
                    item.offset_ms
                );
            }
        }

        if (
            /finished|finish|ended|ends|inactive|finalised|finalized/.test(
                item.status
            )
        ) {
            finishes.push(
                item.offset_ms
            );
        }
    }

    if (!starts.length) {
        return null;
    }

    const startMs =
        starts[0];

    const endCandidate =
        finishes.find(
            value =>
                value >
                startMs +
                30 * 1000
        );

    const endMs =
        Number.isFinite(
            endCandidate
        )
            ? endCandidate
            : Number(
                movement?.end_ms ||
                (
                    startMs +
                    Number(
                        movement?.duration_ms ||
                        0
                    )
                )
            );

    if (
        !Number.isFinite(endMs) ||
        endMs <= startMs
    ) {
        return null;
    }

    return {
        start_ms:
            startMs,

        end_ms:
            endMs,

        duration_ms:
            endMs -
            startMs,

        phase:
            null,

        source:
            "session_status"
    };
}

async function getArchiveBaseSessionWindow(
    basePath,
    movement
) {
    try {
        const statusRecords =
            await getParsedArchiveStream(
                basePath +
                "SessionStatus.jsonStream",
                false
            );

        const window =
            chooseArchiveActiveSessionWindow(
                statusRecords,
                movement
            );

        if (window) {
            return window;
        }
    } catch (error) {
        console.warn(
            "[F1] No se pudo obtener ventana real de sesión:",
            error.message
        );
    }

    return {
        start_ms:
            movement.start_ms,

        end_ms:
            movement.end_ms,

        duration_ms:
            movement.duration_ms,

        phase:
            null,

        source:
            "movement_fallback"
    };
}

async function resolveArchiveReplayWindow(
    basePath,
    positionRecords,
    movement,
    requestedPhase
) {
    const phase =
        String(
            requestedPhase ||
            ""
        )
        .trim()
        .toUpperCase();

    if (phase) {
        const qualifyingPhases =
            await getArchiveQualifyingPhases(
                basePath,
                positionRecords
            );

        const selected =
            selectArchiveReplayWindow(
                movement,
                qualifyingPhases,
                phase
            );

        return {
            ...selected,
            qualifying_phases:
                qualifyingPhases
        };
    }

    const baseWindow =
        await getArchiveBaseSessionWindow(
            basePath,
            movement
        );

    return {
        ...baseWindow,
        qualifying_phases:
            []
    };
}

function selectArchiveReplayWindow(
    movement,
    phases,
    requestedPhase
) {
    const wanted =
        String(
            requestedPhase ||
            ""
        )
        .trim()
        .toUpperCase();

    if (wanted) {
        const phase =
            (phases || [])
            .find(
                item =>
                    String(
                        item.key ||
                        ""
                    )
                    .toUpperCase() ===
                    wanted
            );

        if (phase) {
            const officialDuration =
                getQualifyingPhaseClockMs(
                    phase.key
                );

            return {
                start_ms:
                    phase.start_ms,
                end_ms:
                    Math.max(
                        Number(phase.end_ms || 0),
                        Number(phase.start_ms) +
                        (officialDuration || phase.duration_ms)
                    ),
                duration_ms:
                    officialDuration ||
                    phase.duration_ms,
                phase:
                    phase.key,
                source:
                    phase.source ||
                    "archive"
            };
        }

        /*
           Protección adicional: incluso si ninguna fuente
           pudo detectar la fase, el reloj mostrado debe ser
           el reglamentario y no 60+ minutos.
        */
        const officialDuration =
            getQualifyingPhaseClockMs(
                wanted
            );

        if (
            officialDuration > 0
        ) {
            return {
                start_ms:
                    movement.start_ms,
                end_ms:
                    movement.start_ms +
                    officialDuration,
                duration_ms:
                    officialDuration,
                phase:
                    wanted,
                source:
                    "official_clock_guard"
            };
        }
    }

    return {
        start_ms:
            movement.start_ms,
        end_ms:
            movement.end_ms,
        duration_ms:
            movement.duration_ms,
        phase:
            null,
        source:
            "movement"
    };
}

async function getArchiveDriverListStatic(basePath) {
    try {
        return await getArchiveJson(
            basePath + "DriverList.json"
        );
    } catch (error) {
        return null;
    }
}

const archiveReplayStateCache =
    new Map();

function mergeArchiveRecordsUntil(
    records,
    targetMs,
    previous
) {
    let state =
        previous?.state &&
        Number(previous.target_ms) <=
            Number(targetMs)
            ? previous.state
            : {};

    let index =
        previous?.state &&
        Number(previous.target_ms) <=
            Number(targetMs)
            ? Number(previous.index || 0)
            : 0;

    if (
        !previous ||
        Number(previous.target_ms) >
            Number(targetMs)
    ) {
        state = {};
        index = 0;
    }

    while (
        index < records.length &&
        Number(
            records[index]?.offset_ms
        ) <= Number(targetMs)
    ) {
        const patch =
            records[index]?.data;

        if (
            patch &&
            typeof patch ===
                "object"
        ) {
            state =
                deepMerge(
                    state,
                    patch
                );
        }

        index++;
    }

    return {
        state,
        index,
        target_ms:
            Number(targetMs)
    };
}


const F1_2026_DRIVER_FALLBACK =
    Object.freeze({
        "1":  { abbreviation:"NOR", name:"Lando Norris",      team:"McLaren",         team_color:"#FF8700" },
        "3":  { abbreviation:"VER", name:"Max Verstappen",    team:"Red Bull Racing", team_color:"#3671C6" },
        "5":  { abbreviation:"BOR", name:"Gabriel Bortoleto", team:"Audi",            team_color:"#E30613" },
        "10": { abbreviation:"GAS", name:"Pierre Gasly",      team:"Alpine",          team_color:"#0093CC" },
        "11": { abbreviation:"PER", name:"Sergio Perez",      team:"Cadillac",        team_color:"#B9C0C8" },
        "12": { abbreviation:"ANT", name:"Kimi Antonelli",    team:"Mercedes",        team_color:"#00D2BE" },
        "14": { abbreviation:"ALO", name:"Fernando Alonso",   team:"Aston Martin",    team_color:"#229971" },
        "16": { abbreviation:"LEC", name:"Charles Leclerc",   team:"Ferrari",         team_color:"#E8002D" },
        "18": { abbreviation:"STR", name:"Lance Stroll",      team:"Aston Martin",    team_color:"#229971" },
        "22": { abbreviation:"TSU", name:"Yuki Tsunoda",      team:"Racing Bulls",    team_color:"#6692FF" },
        "23": { abbreviation:"ALB", name:"Alexander Albon",   team:"Williams",        team_color:"#64C4FF" },
        "27": { abbreviation:"HUL", name:"Nico Hulkenberg",   team:"Audi",            team_color:"#E30613" },
        "30": { abbreviation:"LAW", name:"Liam Lawson",       team:"Racing Bulls",    team_color:"#6692FF" },
        "31": { abbreviation:"OCO", name:"Esteban Ocon",      team:"Haas",            team_color:"#B6BABD" },
        "41": { abbreviation:"LIN", name:"Arvid Lindblad",    team:"Red Bull Racing", color:"#3671C6", team_color:"#3671C6" },
        "43": { abbreviation:"COL", name:"Franco Colapinto",  team:"Alpine",          team_color:"#0093CC" },
        "44": { abbreviation:"HAM", name:"Lewis Hamilton",    team:"Ferrari",         team_color:"#E8002D" },
        "55": { abbreviation:"SAI", name:"Carlos Sainz",      team:"Williams",        team_color:"#64C4FF" },
        "63": { abbreviation:"RUS", name:"George Russell",    team:"Mercedes",        team_color:"#00D2BE" },
        "77": { abbreviation:"BOT", name:"Valtteri Bottas",   team:"Cadillac",        team_color:"#B9C0C8" },
        "81": { abbreviation:"PIA", name:"Oscar Piastri",     team:"McLaren",         team_color:"#FF8700" },
        "87": { abbreviation:"BEA", name:"Oliver Bearman",    team:"Haas",            team_color:"#B6BABD" }
    });

function getF12026DriverFallback(number) {
    return F1_2026_DRIVER_FALLBACK[String(number || "")] || null;
}

function normalizeArchiveDriverMap(
    driverState
) {
    const raw =
        driverState?.DriverList ||
        driverState?.driverList ||
        driverState?.Drivers ||
        driverState?.drivers ||
        driverState?.DriverInfo ||
        driverState?.driverInfo ||
        driverState ||
        {};

    const result =
        Object.create(null);

    for (
        const [key, driver]
        of Object.entries(raw)
    ) {
        if (
            !driver ||
            typeof driver !==
                "object"
        ) {
            continue;
        }

        const number =
            String(
                driver.RacingNumber ||
                driver.Number ||
                key
            );

        const fallback =
            getF12026DriverFallback(number);

        result[number] = {
            driver_number:
                number,

            name:
                driver.FullName ||
                driver.BroadcastName ||
                driver.LastName ||
                driver.Tla ||
                fallback?.name ||
                number,

            abbreviation:
                driver.Tla ||
                driver.ShortName ||
                driver.LastName ||
                fallback?.abbreviation ||
                number,

            team:
                driver.TeamName ||
                driver.Team ||
                fallback?.team ||
                "",

            team_color:
                normalizeTeamColour(
                    driver.TeamColour ||
                    driver.TeamColor
                ) ||
                fallback?.team_color ||
                null,

            country_code:
                driver.CountryCode ||
                "",

            headshot_url:
                driver.HeadshotUrl ||
                driver.HeadshotURL ||
                null
        };
    }

    return result;
}


function getArchiveTimingLines(
    timingState
) {
    if (
        !timingState ||
        typeof timingState !==
            "object"
    ) {
        return {};
    }

    const candidates = [
        timingState?.TimingDataF1?.Lines,
        timingState?.TimingDataF1?.lines,
        timingState?.TimingData?.Lines,
        timingState?.TimingData?.lines,
        timingState?.Lines,
        timingState?.lines,
        timingState?.TimingLines,
        timingState?.timingLines
    ];

    for (const candidate of candidates) {
        if (
            candidate &&
            typeof candidate ===
                "object" &&
            !Array.isArray(candidate)
        ) {
            return candidate;
        }
    }

    /*
       Último fallback:
       buscar recursivamente un objeto llamado Lines.
    */
    const stack = [
        timingState
    ];

    const visited =
        new Set();

    while (stack.length) {
        const current =
            stack.shift();

        if (
            !current ||
            typeof current !==
                "object" ||
            visited.has(current)
        ) {
            continue;
        }

        visited.add(current);

        if (
            current.Lines &&
            typeof current.Lines ===
                "object" &&
            !Array.isArray(
                current.Lines
            )
        ) {
            return current.Lines;
        }

        if (
            current.lines &&
            typeof current.lines ===
                "object" &&
            !Array.isArray(
                current.lines
            )
        ) {
            return current.lines;
        }

        for (
            const value of
            Object.values(current)
        ) {
            if (
                value &&
                typeof value ===
                    "object"
            ) {
                stack.push(value);
            }
        }
    }

    return {};
}

function mergeArchiveTimingStates(
    primary,
    secondary
) {
    const first =
        primary &&
        typeof primary ===
            "object"
            ? primary
            : {};

    const second =
        secondary &&
        typeof secondary ===
            "object"
            ? secondary
            : {};

    /*
       TimingDataF1 tiene prioridad cuando contiene el mismo
       campo que TimingData. Los feeds se complementan.
    */
    return deepMerge(
        second,
        first
    );
}

function archiveTimingValue(
    line,
    ...keys
) {
    for (const key of keys) {
        if (
            line &&
            Object.prototype
                .hasOwnProperty
                .call(
                    line,
                    key
                )
        ) {
            return line[key];
        }
    }

    return null;
}

function archiveTimingBestLap(
    line
) {
    const direct =
        archiveTimingValue(
            line,
            "BestLapTime",
            "bestLapTime",
            "BestLap",
            "bestLap"
        );

    const directValue =
        valueOfTime(
            direct
        );

    if (
        directValue !== null &&
        directValue !== undefined &&
        directValue !== ""
    ) {
        return directValue;
    }

    const stats =
        line?.Stats ||
        line?.stats ||
        {};

    return valueOfTime(
        stats?.BestLapTime ||
        stats?.bestLapTime ||
        stats?.BestLap ||
        stats?.bestLap
    );
}

function archiveTimingSectorValues(
    line
) {
    const raw =
        line?.Sectors ||
        line?.sectors ||
        [];

    if (Array.isArray(raw)) {
        return raw;
    }

    if (
        raw &&
        typeof raw ===
            "object"
    ) {
        return [
            raw["0"] ??
                raw[0] ??
                raw.Sector1 ??
                raw.S1 ??
                null,

            raw["1"] ??
                raw[1] ??
                raw.Sector2 ??
                raw.S2 ??
                null,

            raw["2"] ??
                raw[2] ??
                raw.Sector3 ??
                raw.S3 ??
                null
        ];
    }

    return [];
}

function archiveTimingPosition(
    line
) {
    const candidates = [
        line?.Position,
        line?.position,
        line?.Rank,
        line?.rank
    ];

    for (const candidate of candidates) {
        const value =
            Number(
                typeof candidate ===
                    "object"
                    ? (
                        candidate?.Value ??
                        candidate?.value
                    )
                    : candidate
            );

        if (
            Number.isFinite(value)
        ) {
            return value;
        }
    }

    return null;
}

function normalizeArchiveReplayTimingStats(rawState) {
    const rawLines = rawState?.Lines ?? rawState?.lines ?? rawState ?? {};
    const entries = Array.isArray(rawLines)
        ? rawLines.map((line,index) => [String(index), line])
        : Object.entries(rawLines && typeof rawLines === "object" ? rawLines : {});

    const result = {};

    for (const [key,line] of entries) {
        if (!line || typeof line !== "object") continue;
        const number = String(
            line.RacingNumber ?? line.racingNumber ??
            line.DriverNumber ?? line.driverNumber ?? key ?? ""
        ).trim();
        if (!number) continue;

        const rawBest = line.BestSectors ?? line.bestSectors ?? [];
        const best = asArray(rawBest).slice(0,3).map(normalizeTimingStatsValue);

        result[number] = {
            best_lap: normalizeTimingStatsValue(
                line.PersonalBestLapTime ?? line.personalBestLapTime
            ),
            best_sector_1: best[0] ?? null,
            best_sector_2: best[1] ?? null,
            best_sector_3: best[2] ?? null
        };
    }

    return result;
}

function parseArchiveLapTimeMs(value) {
    const text = String(value ?? "").trim();
    if (!text) return null;
    const match = text.match(/^(?:(\d+):)?(\d+)(?:\.(\d+))?$/);
    if (!match) return null;
    const minutes = Number(match[1] || 0);
    const seconds = Number(match[2] || 0);
    const millis = Number(String(match[3] || "0").padEnd(3,"0").slice(0,3));
    return ((minutes * 60) + seconds) * 1000 + millis;
}

function formatArchiveQualiGap(ms) {
    if (!Number.isFinite(ms) || ms < 0) return null;
    return "+" + (ms / 1000).toFixed(3);
}

function normalizeArchiveReplayDrivers(
    timingState,
    driverState,
    positions,
    allowedDriverNumbers = null,
    timingStatsState = null,
    replayPhase = null,
    finalFrame = false
) {
    const lines =
        getArchiveTimingLines(
            timingState
        );

    const drivers =
        normalizeArchiveDriverMap(
            driverState
        );

    const timingStats =
        normalizeArchiveReplayTimingStats(
            timingStatsState
        );

    const allNumbers =
        new Set([
            ...(
                allowedDriverNumbers
                    ? Array.from(
                        allowedDriverNumbers,
                        value =>
                            String(value)
                    )
                    : []
            ),
            ...Object.keys(drivers),
            ...Object.keys(lines),
            ...Object.keys(
                positions || {}
            )
        ]);

    const result =
        [];

    for (const number of allNumbers) {
        const line =
            lines?.[number] ||
            {};

        const fallback =
            getF12026DriverFallback(number);

        const driver =
            drivers?.[number] ||
            {
                driver_number:
                    String(number),

                name:
                    fallback?.name ||
                    String(number),

                abbreviation:
                    fallback?.abbreviation ||
                    String(number),

                team:
                    fallback?.team ||
                    "",

                team_color:
                    fallback?.team_color ||
                    null
            };

        const sectors =
            archiveTimingSectorValues(
                line
            );

        const positionValue =
            archiveTimingPosition(
                line
            );

        result.push({
            driver_number:
                String(number),

            name:
                driver.name,

            abbreviation:
                driver.abbreviation,

            team:
                driver.team,

            team_color:
                driver.team_color,

            country_code:
                driver.country_code ||
                "",

            headshot_url:
                driver.headshot_url ||
                null,

            position:
                Number.isFinite(
                    positionValue
                )
                    ? positionValue
                    : null,

            gap:
                valueOfTime(
                    archiveTimingValue(
                        line,
                        "GapToLeader",
                        "gapToLeader",
                        "gap_to_leader",
                        "Gap",
                        "gap"
                    )
                ),

            interval:
                valueOfTime(
                    archiveTimingValue(
                        line,
                        "IntervalToPositionAhead",
                        "intervalToPositionAhead",
                        "Interval",
                        "interval"
                    )
                ),

            last_lap:
                valueOfTime(
                    archiveTimingValue(
                        line,
                        "LastLapTime",
                        "lastLapTime",
                        "LastLap",
                        "lastLap"
                    )
                ),

            best_lap:
                timingStats?.[number]?.best_lap ||
                archiveTimingBestLap(
                    line
                ),

            sector_1:
                valueOfTime(sectors[0]) ||
                timingStats?.[number]?.best_sector_1 || null,

            sector_2:
                valueOfTime(sectors[1]) ||
                timingStats?.[number]?.best_sector_2 || null,

            sector_3:
                valueOfTime(sectors[2]) ||
                timingStats?.[number]?.best_sector_3 || null,

            best_sector_1:
                timingStats?.[number]?.best_sector_1 || null,

            best_sector_2:
                timingStats?.[number]?.best_sector_2 || null,

            best_sector_3:
                timingStats?.[number]?.best_sector_3 || null,

            in_pit:
                Boolean(
                    line.InPit ??
                    line.inPit ??
                    line.PitOut ??
                    false
                )
        });
    }

    /*
       En el frame FINAL de Qualifying, TimingData puede conservar
       Position de un update previo aunque BestLapTime ya tenga la
       clasificación definitiva. Para Q1/Q2/Q3 ordenamos los pilotos
       que participaron en esa fase por su mejor vuelta oficial.

       Los eliminados de fases anteriores mantienen su posición ya
       establecida y no contaminan el top activo.
    */
    const normalizedReplayPhase =
        String(
            replayPhase ||
            ""
        )
        .trim()
        .toUpperCase();

    if (
        finalFrame &&
        /^(Q|SQ)[123]$/.test(
            normalizedReplayPhase
        )
    ) {
        const activeLimit =
            /3$/.test(normalizedReplayPhase)
                ? 10
                : (
                    /2$/.test(normalizedReplayPhase)
                        ? 16
                        : 22
                );

        const activeFinal =
            result
                .filter(driver => {
                    const currentPosition =
                        Number(
                            driver.position
                        );

                    return (
                        Number.isFinite(
                            currentPosition
                        ) &&
                        currentPosition >= 1 &&
                        currentPosition <= activeLimit &&
                        Number.isFinite(
                            parseArchiveLapTimeMs(
                                driver.best_lap
                            )
                        )
                    );
                })
                .sort((a,b) =>
                    parseArchiveLapTimeMs(
                        a.best_lap
                    ) -
                    parseArchiveLapTimeMs(
                        b.best_lap
                    )
                );

        activeFinal.forEach(
            (driver,index) => {
                driver.position =
                    index + 1;
            }
        );
    }

    result.sort(
        function (a, b) {
            const pa =
                Number(
                    a.position
                );

            const pb =
                Number(
                    b.position
                );

            if (
                Number.isFinite(pa) &&
                Number.isFinite(pb)
            ) {
                return pa - pb;
            }

            if (
                Number.isFinite(pa)
            ) {
                return -1;
            }

            if (
                Number.isFinite(pb)
            ) {
                return 1;
            }

            return (
                Number(
                    a.driver_number
                ) -
                Number(
                    b.driver_number
                )
            );
        }
    );

    const phase = String(replayPhase || "").trim().toUpperCase();
    const activeCount = /3$/.test(phase) ? 10 : (/2$/.test(phase) ? 16 : 22);
    const active = result
        .filter(driver => Number(driver.position) >= 1 && Number(driver.position) <= activeCount)
        .sort((a,b) => Number(a.position) - Number(b.position));

    const leaderMs = active.length ? parseArchiveLapTimeMs(active[0].best_lap) : null;
    let previousMs = null;

    for (const driver of active) {
        const lapMs = parseArchiveLapTimeMs(driver.best_lap);
        if (Number(driver.position) === 1) {
            driver.gap = "LÍDER";
            driver.interval = null;
        } else if (Number.isFinite(lapMs) && Number.isFinite(leaderMs)) {
            if (!driver.gap) driver.gap = formatArchiveQualiGap(lapMs - leaderMs);
            if (!driver.interval && Number.isFinite(previousMs)) {
                driver.interval = formatArchiveQualiGap(lapMs - previousMs);
            }
        }
        if (Number.isFinite(lapMs)) previousMs = lapMs;
    }

    if (/^(Q|SQ)[23]$/.test(phase)) {
        for (const driver of result) {
            if (Number(driver.position) > activeCount) {
                driver.gap = null;
                driver.interval = null;
                driver.last_lap = null;
            }
        }
    }

    return result;
}


function extractArchiveTimingLineMaps(data) {
    const maps = [];

    if (!data || typeof data !== "object") {
        return maps;
    }

    const queue = [data];
    const visited = new Set();

    while (queue.length) {
        const current = queue.shift();

        if (
            !current ||
            typeof current !== "object" ||
            visited.has(current)
        ) {
            continue;
        }

        visited.add(current);

        for (const key of ["Lines","lines","TimingLines","timingLines"]) {
            const value = current[key];

            if (
                value &&
                typeof value === "object" &&
                !Array.isArray(value)
            ) {
                maps.push(value);
            }
        }

        for (const value of Object.values(current)) {
            if (value && typeof value === "object") {
                queue.push(value);
            }
        }
    }

    return maps;
}

function mergeArchiveTimingLinesUntil(records, targetMs, previous) {
    let lines =
        previous?.lines &&
        Number(previous.target_ms) <= Number(targetMs)
            ? previous.lines
            : Object.create(null);

    let index =
        previous?.lines &&
        Number(previous.target_ms) <= Number(targetMs)
            ? Number(previous.index || 0)
            : 0;

    if (
        !previous ||
        Number(previous.target_ms) > Number(targetMs)
    ) {
        lines = Object.create(null);
        index = 0;
    }

    while (
        index < (records || []).length &&
        Number(records[index]?.offset_ms) <= Number(targetMs)
    ) {
        const maps =
            extractArchiveTimingLineMaps(
                records[index]?.data
            );

        for (const map of maps) {
            for (const [number, patch] of Object.entries(map)) {
                if (!patch || typeof patch !== "object") continue;

                const key = String(number);

                lines[key] =
                    deepMerge(
                        lines[key] || {},
                        patch
                    );
            }
        }

        index++;
    }

    return {
        lines,
        index,
        target_ms: Number(targetMs)
    };
}

function mergeArchiveTimingLineMaps(secondary, primary) {
    const result = Object.create(null);

    for (const source of [secondary || {}, primary || {}]) {
        for (const [number, line] of Object.entries(source)) {
            result[number] =
                deepMerge(
                    result[number] || {},
                    line
                );
        }
    }

    return result;
}

async function getArchiveReplayTimingState(
    basePath,
    absoluteTargetMs
) {
    const cacheKey = String(basePath);
    let cache = archiveReplayStateCache.get(cacheKey);

    if (!cache) {
        let timingDataRecords = [];
        let timingDataF1Records = [];
        let timingStatsRecords = [];
        let driverRecords = [];

        try {
            timingDataF1Records =
                await getParsedArchiveStream(
                    basePath + "TimingDataF1.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] Replay TimingDataF1 no disponible:",
                error.message
            );
        }

        try {
            timingDataRecords =
                await getParsedArchiveStream(
                    basePath + "TimingData.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] Replay TimingData no disponible:",
                error.message
            );
        }

        try {
            timingStatsRecords =
                await getParsedArchiveStream(
                    basePath + "TimingStats.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] Replay TimingStats no disponible:",
                error.message
            );
        }

        let driverStatic = null;

        try {
            driverRecords =
                await getParsedArchiveStream(
                    basePath + "DriverList.jsonStream",
                    false
                );
        } catch (error) {
            console.warn(
                "[F1] Replay DriverList.jsonStream no disponible:",
                error.message
            );
        }

        if (!driverRecords.length) {
            driverStatic =
                await getArchiveDriverListStatic(basePath);
        }

        cache = {
            timing_data_records: timingDataRecords,
            timing_data_f1_records: timingDataF1Records,
            timing_stats_records: timingStatsRecords,
            driver_records: driverRecords,
            driver_static: driverStatic,
            timing_data_line_cursor: null,
            timing_data_f1_line_cursor: null,
            timing_stats_cursor: null,
            driver_cursor: null
        };

        archiveReplayStateCache.set(cacheKey, cache);
    }

    cache.timing_data_line_cursor =
        mergeArchiveTimingLinesUntil(
            cache.timing_data_records || [],
            absoluteTargetMs,
            cache.timing_data_line_cursor
        );

    cache.timing_data_f1_line_cursor =
        mergeArchiveTimingLinesUntil(
            cache.timing_data_f1_records || [],
            absoluteTargetMs,
            cache.timing_data_f1_line_cursor
        );

    cache.timing_stats_cursor =
        mergeArchiveRecordsUntil(
            cache.timing_stats_records || [],
            absoluteTargetMs,
            cache.timing_stats_cursor
        );

    cache.driver_cursor =
        mergeArchiveRecordsUntil(
            cache.driver_records || [],
            absoluteTargetMs,
            cache.driver_cursor
        );

    const combinedLines =
        mergeArchiveTimingLineMaps(
            cache.timing_data_line_cursor?.lines || {},
            cache.timing_data_f1_line_cursor?.lines || {}
        );

    const drivers =
        (
            cache.driver_cursor?.state &&
            Object.keys(cache.driver_cursor.state).length
        )
            ? cache.driver_cursor.state
            : (cache.driver_static || {});

    return {
        timing: {
            Lines: combinedLines
        },

        drivers,

        timing_stats:
            cache.timing_stats_cursor?.state || {},

        diagnostics: {
            timing_data_records:
                cache.timing_data_records?.length || 0,

            timing_data_f1_records:
                cache.timing_data_f1_records?.length || 0,

            timing_stats_records:
                cache.timing_stats_records?.length || 0,

            timing_lines:
                Object.keys(combinedLines).length,

            driver_stream_records:
                cache.driver_records?.length || 0,

            driver_static_loaded:
                Boolean(cache.driver_static)
        }
    };
}


/* =========================================================
   V1.41.0 · TRUE QUALIFYING FINAL SNAPSHOT + CHECKERED PLAYBACK

   El slider de Q1/Q2/Q3 conserva el reloj reglamentario
   (18/15/12 min), pero el último punto debe representar el
   estado REAL al cerrar la fase:

   Q1 -> justo antes de iniciar Q2
   Q2 -> justo antes de iniciar Q3
   Q3 -> último update oficial de TimingData/TimingDataF1/
         TimingStats de la sesión completa.

   Esto incluye las vueltas terminadas después de 00:00.
========================================================= */

async function getArchiveReplayFinalTimingOffset(
    basePath
) {
    const candidates = [];

    for (const file of [
        "TimingDataF1.jsonStream",
        "TimingData.jsonStream",
        "TimingStats.jsonStream"
    ]) {
        try {
            const records =
                await getParsedArchiveStream(
                    basePath + file,
                    false
                );

            if (records.length) {
                const last =
                    records[
                        records.length - 1
                    ];

                const offset =
                    Number(
                        last?.offset_ms
                    );

                if (Number.isFinite(offset)) {
                    candidates.push(offset);
                }
            }
        } catch (error) {
            console.warn(
                "[F1] No se pudo obtener último offset de " +
                file +
                ":",
                error.message
            );
        }
    }

    return candidates.length
        ? Math.max(...candidates)
        : null;
}

async function getArchiveReplayLastTimingOffsetInRange(
    basePath,
    minOffsetMs,
    maxOffsetMs
) {
    const candidates = [];

    const minMs =
        Number.isFinite(Number(minOffsetMs))
            ? Number(minOffsetMs)
            : -Infinity;

    const maxMs =
        Number.isFinite(Number(maxOffsetMs))
            ? Number(maxOffsetMs)
            : Infinity;

    for (const file of [
        "TimingDataF1.jsonStream",
        "TimingData.jsonStream",
        "TimingStats.jsonStream"
    ]) {
        try {
            const records =
                await getParsedArchiveStream(
                    basePath + file,
                    false
                );

            for (
                let index = records.length - 1;
                index >= 0;
                index--
            ) {
                const offset =
                    Number(
                        records[index]?.offset_ms
                    );

                if (!Number.isFinite(offset)) {
                    continue;
                }

                if (offset >= maxMs) {
                    continue;
                }

                if (offset < minMs) {
                    break;
                }

                candidates.push(offset);
                break;
            }
        } catch (error) {
            console.warn(
                "[F1] No se pudo buscar cierre de fase en " +
                file +
                ":",
                error.message
            );
        }
    }

    return candidates.length
        ? Math.max(...candidates)
        : null;
}


async function resolveArchiveQualifyingFinalTarget(
    basePath,
    replayWindow,
    requestedPhase
) {
    const phase =
        String(
            requestedPhase ||
            replayWindow?.phase ||
            ""
        )
        .trim()
        .toUpperCase();

    if (!/^(Q|SQ)[123]$/.test(phase)) {
        return Number(
            replayWindow?.end_ms
        );
    }

    const phases =
        Array.isArray(
            replayWindow?.qualifying_phases
        )
            ? replayWindow.qualifying_phases
            : [];

    const phaseIndex =
        Number(
            phase.replace(/\D/g, "")
        ) - 1;

    /*
       V1.41.0 · Q1/Q2

       El reloj oficial puede llegar a cero mientras todavía hay
       pilotos en una vuelta válida. No saltamos directamente al
       snapshot final.

       Buscamos el ÚLTIMO update de TimingData/TimingDataF1/
       TimingStats posterior al final reglamentario y anterior al
       comienzo de la siguiente fase. Ese será el final REAL del
       Replay de esta fase.

       Así el frontend puede reproducir, uno por uno, los tiempos
       que se registran después de la bandera a cuadros.
    */
    if (
        phaseIndex >= 0 &&
        phaseIndex < 2 &&
        phases[phaseIndex + 1] &&
        Number.isFinite(
            Number(
                phases[phaseIndex + 1].start_ms
            )
        )
    ) {
        const officialEndMs =
            Number(replayWindow.start_ms) +
            Number(replayWindow.duration_ms || 0);

        const nextPhaseStartMs =
            Number(
                phases[phaseIndex + 1].start_ms
            );

        const lastTimingOffset =
            await getArchiveReplayLastTimingOffsetInRange(
                basePath,
                officialEndMs,
                nextPhaseStartMs
            );

        if (
            Number.isFinite(
                Number(lastTimingOffset)
            )
        ) {
            return Math.max(
                officialEndMs,
                Number(lastTimingOffset)
            );
        }

        return Math.max(
            officialEndMs,
            nextPhaseStartMs - 1
        );
    }

    /*
       Q3: necesitamos el último update oficial del archivo.
       SessionStatus puede marcar FINISHED cuando el reloj llega
       a cero, pero TimingData sigue recibiendo las vueltas que
       estaban en curso.
    */
    if (phaseIndex === 2) {
        const lastTimingOffset =
            await getArchiveReplayFinalTimingOffset(
                basePath
            );

        if (
            Number.isFinite(
                lastTimingOffset
            )
        ) {
            return Math.max(
                Number(replayWindow.end_ms || 0),
                lastTimingOffset
            );
        }
    }

    return Number(
        replayWindow.end_ms
    );
}


app.get("/api/archive/replay-frame", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        const relativeMs =
            Number(
                req.query.ms || 0
            );

        if (
            !sessionPath ||
            !Number.isFinite(
                relativeMs
            ) ||
            relativeMs < 0
        ) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Ruta de sesión o tiempo inválido."
                });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        let positionRecords = [];

        try {
            positionRecords =
                await getParsedArchiveStream(
                    basePath +
                    "Position.z.jsonStream",
                    true
                );
        } catch (error) {
            console.warn(
                "[F1] Replay sin Position.z; continuando con Timing:",
                error.message
            );
        }

        const allowed =
            getArchiveDriverNumbers();

        const referenceDriver =
            chooseArchiveTrackDriver(
                positionRecords,
                allowed
            );

        const referencePoints =
            referenceDriver
                ? collectArchiveDriverTrackPoints(
                    positionRecords,
                    referenceDriver
                )
                : [];

        let movement =
            getReplayMovementMeta(
                referencePoints
            );

        if (
            !movement ||
            !Number.isFinite(Number(movement.start_ms)) ||
            !Number.isFinite(Number(movement.end_ms)) ||
            Number(movement.end_ms) <= Number(movement.start_ms)
        ) {
            /*
               Timing-only replay fallback. SessionData defines the real
               Q1/Q2/Q3 windows when Position.z is not yet in the archive.
            */
            const fallbackPhases =
                await getArchiveQualifyingPhases(
                    basePath,
                    []
                );

            if (fallbackPhases.length) {
                movement = {
                    start_ms:
                        Math.min(
                            ...fallbackPhases.map(
                                item => Number(item.start_ms)
                            )
                        ),
                    end_ms:
                        Math.max(
                            ...fallbackPhases.map(
                                item => Number(item.end_ms)
                            )
                        )
                };

                movement.duration_ms =
                    movement.end_ms -
                    movement.start_ms;
            }
        }

        if (
            !movement ||
            !Number.isFinite(Number(movement.start_ms)) ||
            !Number.isFinite(Number(movement.end_ms))
        ) {
            throw new Error(
                "El archivo de la sesión todavía no contiene una ventana de Replay utilizable."
            );
        }

        const replayWindow =
            await resolveArchiveReplayWindow(
                basePath,
                positionRecords,
                movement,
                req.query.phase
            );

        const qualifyingPhases =
            replayWindow
                .qualifying_phases ||
            [];

        /*
           V1.41.0 · NO saltar al resultado final cuando termina
           el reloj reglamentario.

           Primero calculamos hasta qué instante hay que reproducir
           para ver terminar las vueltas válidas abiertas antes de
           la bandera a cuadros. Después avanzamos cronológicamente
           hasta ese punto.
        */
        const trueFinalTargetMs =
            await resolveArchiveQualifyingFinalTarget(
                basePath,
                replayWindow,
                req.query.phase
            );

        const playbackDurationMs =
            Math.max(
                Number(
                    replayWindow.duration_ms ||
                    0
                ),
                Number.isFinite(
                    Number(
                        trueFinalTargetMs
                    )
                )
                    ? Math.max(
                        0,
                        Number(
                            trueFinalTargetMs
                        ) -
                        Number(
                            replayWindow.start_ms
                        )
                      )
                    : 0
            );

        const safeRelativeMs =
            Math.min(
                relativeMs,
                playbackDurationMs
            );

        const isReplayPhaseEnd =
            safeRelativeMs >=
            playbackDurationMs;

        const absoluteReplayTargetMs =
            Number(
                replayWindow.start_ms
            ) +
            safeRelativeMs;

        const target =
            positionRecords.length
                ? findArchiveRecordAtOrBeforeRelative(
                    positionRecords,
                    replayWindow.start_ms,
                    Math.max(0, absoluteReplayTargetMs - replayWindow.start_ms)
                )
                : {
                    record: null,
                    absolute_target_ms:
                        absoluteReplayTargetMs
                };

        const positions =
            target.record
                ? extractPositionsFromArchiveRecord(
                    target.record,
                    allowed
                )
                : {};

        const replayState =
            await getArchiveReplayTimingState(
                basePath,
                target.absolute_target_ms
            );

        const drivers =
            normalizeArchiveReplayDrivers(
                replayState.timing,
                replayState.drivers,
                positions,
                allowed,
                replayState.timing_stats,
                replayWindow.phase,
                isReplayPhaseEnd
            );

        res.set(
            "Cache-Control",
            "no-store"
        );

        res.json({
            success: true,
            path:
                basePath,

            requested_ms:
                relativeMs,

            replay_start_ms:
                replayWindow.start_ms,

            replay_end_ms:
                replayWindow.end_ms,

            duration_ms:
                replayWindow.duration_ms,

            official_duration_ms:
                replayWindow.duration_ms,

            playback_duration_ms:
                playbackDurationMs,

            replay_phase:
                replayWindow.phase,

            replay_phase_source:
                replayWindow.source ||
                null,

            replay_final_frame:
                isReplayPhaseEnd,

            replay_final_target_ms:
                isReplayPhaseEnd
                    ? absoluteReplayTargetMs
                    : null,

            session_relative_ms:
                Math.max(
                    0,
                    target.absolute_target_ms -
                    replayWindow.start_ms
                ),

            qualifying_phases:
                qualifyingPhases,

            absolute_target_ms:
                target.absolute_target_ms,

            position_offset:
                target.record?.offset ||
                null,

            position_offset_ms:
                target.record
                    ?.offset_ms ??
                null,

            drivers,

            replay_timing_diagnostics:
                replayState.diagnostics ||
                null,

            positions
        });

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});




/* =========================================================
   V1.15.0 · ANÁLISIS HISTÓRICO DE SESIÓN
========================================================= */

function f1AnalysisTimeToSeconds(value) {
    const text =
        String(
            valueOfTime(value) ??
            value ??
            ""
        ).trim();

    if (!text) return null;

    const parts =
        text.split(":");

    let seconds;

    if (parts.length === 2) {
        seconds =
            Number(parts[0]) * 60 +
            Number(parts[1]);
    } else {
        seconds =
            Number(parts[0]);
    }

    return Number.isFinite(seconds)
        ? seconds
        : null;
}

function f1AnalysisFormatSeconds(seconds) {
    const value =
        Number(seconds);

    if (!Number.isFinite(value)) {
        return null;
    }

    const minutes =
        Math.floor(value / 60);

    const remaining =
        value - minutes * 60;

    return (
        minutes > 0
            ? `${minutes}:${remaining.toFixed(3).padStart(6, "0")}`
            : remaining.toFixed(3)
    );
}

function extractF1AnalysisStints(state) {
    const candidates = [
        state?.TimingAppData?.Lines,
        state?.TimingAppData?.lines,
        state?.Lines,
        state?.lines
    ];

    let lines = null;

    for (const candidate of candidates) {
        if (
            candidate &&
            typeof candidate === "object" &&
            !Array.isArray(candidate)
        ) {
            lines = candidate;
            break;
        }
    }

    if (!lines) {
        const queue = [state];
        const seen = new Set();

        while (queue.length && !lines) {
            const current = queue.shift();

            if (
                !current ||
                typeof current !== "object" ||
                seen.has(current)
            ) continue;

            seen.add(current);

            if (
                current.Lines &&
                typeof current.Lines === "object" &&
                !Array.isArray(current.Lines)
            ) {
                lines = current.Lines;
                break;
            }

            for (const value of Object.values(current)) {
                if (value && typeof value === "object") {
                    queue.push(value);
                }
            }
        }
    }

    const result =
        Object.create(null);

    for (const [number, line] of Object.entries(lines || {})) {
        const raw =
            line?.Stints ||
            line?.stints ||
            [];

        const list =
            Array.isArray(raw)
                ? raw
                : (
                    raw &&
                    typeof raw === "object"
                        ? Object.values(raw)
                        : []
                );

        result[String(number)] =
            list
                .filter(item => item && typeof item === "object")
                .map(item => ({
                    compound:
                        item.Compound ??
                        item.compound ??
                        null,

                    laps:
                        item.TotalLaps ??
                        item.LapCount ??
                        item.Laps ??
                        item.laps ??
                        null,

                    new:
                        item.New ??
                        item.new ??
                        null
                }));
    }

    return result;
}

function getF1AnalysisSpeedTrap(line) {
    const speeds =
        line?.Speeds ||
        line?.speeds ||
        {};

    const candidates = [];

    const walk = value => {
        if (value === null || value === undefined) {
            return;
        }

        if (
            typeof value === "number" ||
            typeof value === "string"
        ) {
            const n = Number(value);

            if (
                Number.isFinite(n) &&
                n > 100 &&
                n < 450
            ) {
                candidates.push(n);
            }

            return;
        }

        if (typeof value === "object") {
            if (
                Object.prototype.hasOwnProperty.call(value, "Value") ||
                Object.prototype.hasOwnProperty.call(value, "value")
            ) {
                walk(
                    value.Value ??
                    value.value
                );
            }

            for (const item of Object.values(value)) {
                walk(item);
            }
        }
    };

    walk(speeds);

    return candidates.length
        ? Math.max(...candidates)
        : null;
}

function mergeF1AnalysisTimingRecord(lines, record) {
    const maps =
        extractArchiveTimingLineMaps(
            record?.data
        );

    const touched = new Set();

    for (const map of maps) {
        for (const [number, patch] of Object.entries(map)) {
            if (!patch || typeof patch !== "object") {
                continue;
            }

            const key =
                String(number);

            lines[key] =
                deepMerge(
                    lines[key] || {},
                    patch
                );

            touched.add(key);
        }
    }

    return touched;
}


function normalizeF1AnalysisGapSeconds(value) {
    const raw =
        valueOfTime(value);

    if (
        raw === null ||
        raw === undefined ||
        raw === ""
    ) {
        return null;
    }

    const text =
        String(raw)
            .trim()
            .replace(/^\+/, "");

    if (
        /lap/i.test(text) ||
        /pit/i.test(text)
    ) {
        return null;
    }

    const numeric =
        Number(text);

    if (Number.isFinite(numeric)) {
        return numeric;
    }

    return f1AnalysisTimeToSeconds(
        text
    );
}

function extractF1AnalysisLapCountState(state) {
    if (!state || typeof state !== "object") {
        return {
            current_lap:null,
            total_laps:null
        };
    }

    const candidates = [
        state,
        state.LapCount,
        state.lapCount
    ].filter(Boolean);

    for (const item of candidates) {
        const current =
            Number(
                item.CurrentLap ??
                item.currentLap ??
                item.Lap ??
                item.lap
            );

        const total =
            Number(
                item.TotalLaps ??
                item.totalLaps ??
                item.Laps ??
                item.laps
            );

        if (
            Number.isFinite(current) ||
            Number.isFinite(total)
        ) {
            return {
                current_lap:
                    Number.isFinite(current)
                        ? current
                        : null,

                total_laps:
                    Number.isFinite(total)
                        ? total
                        : null
            };
        }
    }

    const queue = [state];
    const seen = new Set();

    while (queue.length) {
        const current = queue.shift();

        if (
            !current ||
            typeof current !== "object" ||
            seen.has(current)
        ) {
            continue;
        }

        seen.add(current);

        const total =
            Number(
                current.TotalLaps ??
                current.totalLaps
            );

        const lap =
            Number(
                current.CurrentLap ??
                current.currentLap
            );

        if (
            Number.isFinite(total) ||
            Number.isFinite(lap)
        ) {
            return {
                current_lap:
                    Number.isFinite(lap)
                        ? lap
                        : null,

                total_laps:
                    Number.isFinite(total)
                        ? total
                        : null
            };
        }

        for (const value of Object.values(current)) {
            if (value && typeof value === "object") {
                queue.push(value);
            }
        }
    }

    return {
        current_lap:null,
        total_laps:null
    };
}

function extractF1PitLaneTimes(state) {
    const result =
        Object.create(null);

    if (!state || typeof state !== "object") {
        return result;
    }

    function addPit(
        number,
        item
    ) {
        const driverNumber =
            String(
                number ||
                item?.RacingNumber ||
                item?.DriverNumber ||
                item?.Number ||
                ""
            ).trim();

        if (!driverNumber) {
            return;
        }

        const duration =
            valueOfTime(
                item?.Duration ??
                item?.PitLaneTime ??
                item?.PitTime ??
                item?.Time ??
                item?.Value ??
                item?.duration ??
                item?.pitLaneTime ??
                item?.pitTime ??
                item?.time ??
                item?.value
            );

        const lapRaw =
            item?.Lap ??
            item?.LapNumber ??
            item?.lap ??
            item?.lapNumber ??
            null;

        const lap =
            Number(
                typeof lapRaw === "object"
                    ? (
                        lapRaw?.Value ??
                        lapRaw?.value
                    )
                    : lapRaw
            );

        if (!result[driverNumber]) {
            result[driverNumber] = [];
        }

        const fingerprint =
            [
                driverNumber,
                Number.isFinite(lap) ? lap : "",
                duration ?? "",
                item?.Utc ?? item?.Timestamp ?? item?.utc ?? item?.timestamp ?? ""
            ].join("|");

        if (
            result[driverNumber].some(
                pit =>
                    pit.fingerprint === fingerprint
            )
        ) {
            return;
        }

        result[driverNumber].push({
            fingerprint,
            lap:
                Number.isFinite(lap)
                    ? lap
                    : null,
            duration:
                duration ?? null,
            utc:
                item?.Utc ??
                item?.Timestamp ??
                item?.utc ??
                item?.timestamp ??
                null
        });
    }

    const queue = [
        {
            value:state,
            keyHint:""
        }
    ];

    const seen = new Set();

    while (queue.length) {
        const entry =
            queue.shift();

        const current =
            entry.value;

        if (
            !current ||
            typeof current !== "object" ||
            seen.has(current)
        ) {
            continue;
        }

        seen.add(current);

        if (Array.isArray(current)) {
            current.forEach(
                item => {
                    if (
                        item &&
                        typeof item === "object"
                    ) {
                        addPit(
                            entry.keyHint,
                            item
                        );

                        queue.push({
                            value:item,
                            keyHint:
                                entry.keyHint
                        });
                    }
                }
            );

            continue;
        }

        const possibleNumber =
            current.RacingNumber ??
            current.DriverNumber ??
            current.Number ??
            null;

        const hasTimeField =
            [
                "Duration",
                "PitLaneTime",
                "PitTime",
                "Time",
                "Value",
                "duration",
                "pitLaneTime",
                "pitTime",
                "time",
                "value"
            ].some(
                key =>
                    Object.prototype
                        .hasOwnProperty
                        .call(
                            current,
                            key
                        )
            );

        if (
            possibleNumber &&
            hasTimeField
        ) {
            addPit(
                possibleNumber,
                current
            );
        }

        for (const [key, value] of Object.entries(current)) {
            if (!value || typeof value !== "object") {
                continue;
            }

            const nextHint =
                /^\d+$/.test(String(key))
                    ? String(key)
                    : entry.keyHint;

            if (
                /^\d+$/.test(String(key))
            ) {
                if (Array.isArray(value)) {
                    value.forEach(
                        item => {
                            if (
                                item &&
                                typeof item === "object"
                            ) {
                                addPit(
                                    key,
                                    item
                                );
                            }
                        }
                    );
                } else {
                    addPit(
                        key,
                        value
                    );
                }
            }

            queue.push({
                value,
                keyHint:nextHint
            });
        }
    }

    for (const pits of Object.values(result)) {
        pits.sort(
            (a,b) =>
                Number(a.lap || 9999) -
                Number(b.lap || 9999)
        );

        pits.forEach(
            pit => {
                delete pit.fingerprint;
            }
        );
    }

    return result;
}

function extractF1TyreStintSeriesState(state) {
    if (!state || typeof state !== "object") {
        return {};
    }

    const raw =
        state.Stints ||
        state.stints ||
        state.TyreStintSeries?.Stints ||
        state.tyreStintSeries?.stints ||
        null;

    if (!raw || typeof raw !== "object") {
        return {};
    }

    const result =
        Object.create(null);

    for (const [number, value] of Object.entries(raw)) {
        const list =
            Array.isArray(value)
                ? value
                : (
                    value &&
                    typeof value === "object"
                        ? Object.values(value)
                        : []
                );

        result[String(number)] =
            list
                .filter(
                    item =>
                        item &&
                        typeof item === "object"
                )
                .map(
                    item => ({
                        compound:
                            item.Compound ??
                            item.compound ??
                            null,

                        laps:
                            Number(
                                item.TotalLaps ??
                                item.totalLaps ??
                                item.LapCount ??
                                item.lapCount ??
                                0
                            ),

                        start_laps:
                            Number(
                                item.StartLaps ??
                                item.startLaps ??
                                0
                            ),

                        new:
                            item.New ??
                            item.new ??
                            null
                    })
                );
    }

    return result;
}

async function buildF1ArchiveSessionAnalysis(basePath) {
    let timingData = [];
    let timingDataF1 = [];
    let timingAppData = [];
    let driverRecords = [];
    let lapCountRecords = [];
    let pitLaneRecords = [];
    let tyreStintRecords = [];
    let sessionInfo = null;
    let driverStatic = null;

    try {
        timingData =
            await getParsedArchiveStream(
                basePath + "TimingData.jsonStream",
                false
            );
    } catch (error) {}

    try {
        timingDataF1 =
            await getParsedArchiveStream(
                basePath + "TimingDataF1.jsonStream",
                false
            );
    } catch (error) {}

    try {
        timingAppData =
            await getParsedArchiveStream(
                basePath + "TimingAppData.jsonStream",
                false
            );
    } catch (error) {}

    try {
        driverRecords =
            await getParsedArchiveStream(
                basePath + "DriverList.jsonStream",
                false
            );
    } catch (error) {}

    try {
        lapCountRecords =
            await getParsedArchiveStream(
                basePath + "LapCount.jsonStream",
                false
            );
    } catch (error) {}

    try {
        pitLaneRecords =
            await getParsedArchiveStream(
                basePath + "PitLaneTimeCollection.jsonStream",
                false
            );
    } catch (error) {}

    try {
        tyreStintRecords =
            await getParsedArchiveStream(
                basePath + "TyreStintSeries.jsonStream",
                false
            );
    } catch (error) {}

    try {
        sessionInfo =
            await getArchiveJson(
                basePath + "SessionInfo.json"
            );
    } catch (error) {}

    driverStatic =
        await getArchiveDriverListStatic(
            basePath
        );

    const driverCursor =
        mergeArchiveRecordsUntil(
            driverRecords,
            Number.MAX_SAFE_INTEGER,
            null
        );

    const driverState =
        (
            driverCursor?.state &&
            Object.keys(driverCursor.state).length
        )
            ? driverCursor.state
            : (driverStatic || {});

    const driverMap =
        normalizeArchiveDriverMap(
            driverState
        );

    const events = [
        ...timingData.map(record => ({
            ...record,
            priority:0
        })),
        ...timingDataF1.map(record => ({
            ...record,
            priority:1
        }))
    ].sort((a,b) => {
        const delta =
            Number(a.offset_ms || 0) -
            Number(b.offset_ms || 0);

        return delta ||
            Number(a.priority || 0) -
            Number(b.priority || 0);
    });

    const lines =
        Object.create(null);

    const lapSeries =
        Object.create(null);

    const lastLapFingerprint =
        Object.create(null);

    const positionHistory =
        Object.create(null);

    const gapHistory =
        Object.create(null);

    const positionFingerprint =
        Object.create(null);

    const gapFingerprint =
        Object.create(null);

    for (const record of events) {
        const touched =
            mergeF1AnalysisTimingRecord(
                lines,
                record
            );

        for (const number of touched) {
            const line =
                lines[number] || {};

            const historyLapRaw =
                archiveTimingValue(
                    line,
                    "NumberOfLaps",
                    "numberOfLaps",
                    "LapNumber",
                    "lapNumber",
                    "Lap",
                    "lap"
                );

            const historyLap =
                Number(
                    typeof historyLapRaw === "object"
                        ? (
                            historyLapRaw?.Value ??
                            historyLapRaw?.value
                        )
                        : historyLapRaw
                );

            const historyPosition =
                archiveTimingPosition(
                    line
                );

            if (
                Number.isFinite(historyLap) &&
                historyLap >= 0 &&
                Number.isFinite(historyPosition)
            ) {
                if (!positionHistory[number]) {
                    positionHistory[number] = [];
                }

                const fp =
                    historyLap +
                    "|" +
                    historyPosition;

                if (
                    positionFingerprint[number] !== fp
                ) {
                    positionHistory[number].push({
                        lap:historyLap,
                        position:historyPosition,
                        offset_ms:
                            Number(record.offset_ms || 0)
                    });

                    positionFingerprint[number] =
                        fp;
                }
            }

            const historyGap =
                normalizeF1AnalysisGapSeconds(
                    archiveTimingValue(
                        line,
                        "GapToLeader",
                        "gapToLeader",
                        "gap_to_leader",
                        "Gap",
                        "gap"
                    )
                );

            if (
                Number.isFinite(historyLap) &&
                historyLap >= 0 &&
                Number.isFinite(historyGap)
            ) {
                if (!gapHistory[number]) {
                    gapHistory[number] = [];
                }

                const fp =
                    historyLap +
                    "|" +
                    historyGap;

                if (
                    gapFingerprint[number] !== fp
                ) {
                    gapHistory[number].push({
                        lap:historyLap,
                        gap_seconds:historyGap,
                        offset_ms:
                            Number(record.offset_ms || 0)
                    });

                    gapFingerprint[number] =
                        fp;
                }
            }

            const lapTime =
                valueOfTime(
                    archiveTimingValue(
                        line,
                        "LastLapTime",
                        "lastLapTime",
                        "LastLap",
                        "lastLap"
                    )
                );

            if (
                lapTime === null ||
                lapTime === undefined ||
                lapTime === ""
            ) {
                continue;
            }

            const lapNumberRaw =
                archiveTimingValue(
                    line,
                    "NumberOfLaps",
                    "numberOfLaps",
                    "LapNumber",
                    "lapNumber",
                    "Lap",
                    "lap"
                );

            const lapNumber =
                Number(
                    typeof lapNumberRaw === "object"
                        ? (
                            lapNumberRaw?.Value ??
                            lapNumberRaw?.value
                        )
                        : lapNumberRaw
                );

            const fingerprint =
                String(lapNumber) +
                "|" +
                String(lapTime);

            if (
                lastLapFingerprint[number] ===
                fingerprint
            ) {
                continue;
            }

            const seconds =
                f1AnalysisTimeToSeconds(
                    lapTime
                );

            if (
                !Number.isFinite(seconds) ||
                seconds <= 0
            ) {
                continue;
            }

            if (!lapSeries[number]) {
                lapSeries[number] = [];
            }

            lapSeries[number].push({
                lap:
                    Number.isFinite(lapNumber)
                        ? lapNumber
                        : lapSeries[number].length + 1,

                time:
                    String(lapTime),

                seconds,

                offset_ms:
                    Number(record.offset_ms || 0)
            });

            lastLapFingerprint[number] =
                fingerprint;
        }
    }

    const appCursor =
        mergeArchiveRecordsUntil(
            timingAppData,
            Number.MAX_SAFE_INTEGER,
            null
        );

    const timingAppStints =
        extractF1AnalysisStints(
            appCursor?.state || {}
        );

    const tyreStintCursor =
        mergeArchiveRecordsUntil(
            tyreStintRecords,
            Number.MAX_SAFE_INTEGER,
            null
        );

    const tyreSeriesStints =
        extractF1TyreStintSeriesState(
            tyreStintCursor?.state || {}
        );

    const stints =
        Object.keys(tyreSeriesStints).length
            ? tyreSeriesStints
            : timingAppStints;

    const lapCountCursor =
        mergeArchiveRecordsUntil(
            lapCountRecords,
            Number.MAX_SAFE_INTEGER,
            null
        );

    const lapCountState =
        extractF1AnalysisLapCountState(
            lapCountCursor?.state || {}
        );

    const pitLaneCursor =
        mergeArchiveRecordsUntil(
            pitLaneRecords,
            Number.MAX_SAFE_INTEGER,
            null
        );

    const pitLaneTimes =
        extractF1PitLaneTimes(
            pitLaneCursor?.state || {}
        );

    const bestSectors = {
        s1:null,
        s2:null,
        s3:null
    };

    const drivers = [];

    function extractAnalysisGap(line) {
        return valueOfTime(
            archiveTimingValue(
                line,
                "GapToLeader",
                "gapToLeader",
                "gap_to_leader",
                "Gap",
                "gap"
            )
        );
    }

    function extractAnalysisInterval(line) {
        return valueOfTime(
            archiveTimingValue(
                line,
                "IntervalToPositionAhead",
                "intervalToPositionAhead",
                "Interval",
                "interval"
            )
        );
    }

    function extractAnalysisPitCount(line) {
        const raw =
            archiveTimingValue(
                line,
                "NumberOfPitStops",
                "PitStopCount",
                "pitStopCount",
                "numberOfPitStops"
            );

        const value =
            Number(
                typeof raw === "object"
                    ? (
                        raw?.Value ??
                        raw?.value
                    )
                    : raw
            );

        return Number.isFinite(value)
            ? value
            : 0;
    }

    function extractAnalysisPitTime(line) {
        const candidates = [
            line?.PitStopTime,
            line?.pitStopTime,
            line?.LastPitStopTime,
            line?.lastPitStopTime,
            line?.PitTime,
            line?.pitTime
        ];

        for (const candidate of candidates) {
            const value =
                valueOfTime(candidate);

            if (
                value !== null &&
                value !== undefined &&
                value !== ""
            ) {
                return value;
            }
        }

        return null;
    }

    function extractAnalysisMiniSectors(line) {
        const sectors =
            archiveTimingSectorValues(
                line
            );

        const output = [];

        sectors.slice(0, 3).forEach(
            (sector, sectorIndex) => {
                const segments =
                    Array.isArray(sector?.Segments)
                        ? sector.Segments
                        : (
                            sector?.Segments &&
                            typeof sector.Segments === "object"
                                ? Object.values(sector.Segments)
                                : (
                                    Array.isArray(sector?.segments)
                                        ? sector.segments
                                        : (
                                            sector?.segments &&
                                            typeof sector.segments === "object"
                                                ? Object.values(sector.segments)
                                                : []
                                        )
                                )
                        );

                segments.forEach(
                    (segment, segmentIndex) => {
                        const raw =
                            valueOfTime(
                                segment?.Value ??
                                segment?.value ??
                                segment
                            );

                        const status =
                            segment?.Status ??
                            segment?.status ??
                            null;

                        output.push({
                            sector:
                                sectorIndex + 1,
                            index:
                                segmentIndex + 1,
                            value:
                                raw,
                            status
                        });
                    }
                );
            }
        );

        return output;
    }

    const numbers =
        new Set([
            ...Object.keys(lines),
            ...Object.keys(driverMap),
            ...Object.keys(lapSeries),
            ...Object.keys(stints)
        ]);

    for (const number of numbers) {
        const line =
            lines[number] || {};

        const fallback =
            getF12026DriverFallback(
                number
            );

        const meta =
            driverMap[number] || {
                driver_number:String(number),
                abbreviation:
                    fallback?.abbreviation ||
                    String(number),
                name:
                    fallback?.name ||
                    String(number),
                team:
                    fallback?.team ||
                    "",
                team_color:
                    fallback?.team_color ||
                    null
            };

        const sectorValues =
            archiveTimingSectorValues(
                line
            );

        const normalizedSectors =
            sectorValues.slice(0,3).map(
                value => {
                    const raw =
                        valueOfTime(value);

                    return {
                        raw,
                        seconds:
                            f1AnalysisTimeToSeconds(
                                raw
                            )
                    };
                }
            );

        normalizedSectors.forEach(
            (sector, index) => {
                if (
                    !Number.isFinite(sector.seconds) ||
                    sector.seconds <= 0
                ) {
                    return;
                }

                const key =
                    "s" + (index + 1);

                if (
                    !bestSectors[key] ||
                    sector.seconds <
                        bestSectors[key].seconds
                ) {
                    bestSectors[key] = {
                        time:
                            String(sector.raw),
                        seconds:
                            sector.seconds,
                        driver:
                            meta.abbreviation ||
                            String(number)
                    };
                }
            }
        );

        const laps =
            lapSeries[number] || [];

        const validLongRun =
            laps
                .filter(
                    lap =>
                        Number.isFinite(lap.seconds) &&
                        lap.seconds > 20 &&
                        lap.seconds < 300
                )
                .slice(-8);

        let longRunAverageSeconds =
            null;

        if (validLongRun.length >= 3) {
            longRunAverageSeconds =
                validLongRun.reduce(
                    (sum, lap) =>
                        sum + lap.seconds,
                    0
                ) /
                validLongRun.length;
        }

        const bestLap =
            archiveTimingBestLap(
                line
            ) ||
            (
                laps.length
                    ? laps.reduce(
                        (best, lap) =>
                            !best ||
                            lap.seconds <
                                best.seconds
                                ? lap
                                : best,
                        null
                    )?.time
                    : null
            );

        drivers.push({
            driver_number:
                String(number),

            abbreviation:
                meta.abbreviation ||
                String(number),

            name:
                meta.name ||
                String(number),

            team:
                meta.team ||
                "",

            team_color:
                meta.team_color ||
                null,

            position:
                archiveTimingPosition(
                    line
                ),

            completed_laps:
                Number(
                    archiveTimingValue(
                        line,
                        "NumberOfLaps",
                        "numberOfLaps",
                        "LapNumber",
                        "lapNumber"
                    )?.Value ??
                    archiveTimingValue(
                        line,
                        "NumberOfLaps",
                        "numberOfLaps",
                        "LapNumber",
                        "lapNumber"
                    )?.value ??
                    archiveTimingValue(
                        line,
                        "NumberOfLaps",
                        "numberOfLaps",
                        "LapNumber",
                        "lapNumber"
                    ) ??
                    0
                ) || 0,

            retired:
                Boolean(
                    line.Retired ??
                    line.retired ??
                    false
                ),

            stopped:
                Boolean(
                    line.Stopped ??
                    line.stopped ??
                    false
                ),

            best_lap:
                bestLap ||
                null,

            gap:
                extractAnalysisGap(
                    line
                ),

            interval:
                extractAnalysisInterval(
                    line
                ),

            pit_count:
                extractAnalysisPitCount(
                    line
                ),

            pit_stop_time:
                extractAnalysisPitTime(
                    line
                ),

            pit_stops:
                pitLaneTimes[number] ||
                [],

            pit_lane_total_seconds:
                (pitLaneTimes[number] || [])
                    .map(
                        pit =>
                            f1AnalysisTimeToSeconds(
                                pit.duration
                            )
                    )
                    .filter(Number.isFinite)
                    .reduce(
                        (sum, value) =>
                            sum + value,
                        0
                    ) ||
                null,

            position_history:
                positionHistory[number] ||
                [],

            gap_history:
                gapHistory[number] ||
                [],

            mini_sectors:
                extractAnalysisMiniSectors(
                    line
                ),

            sectors:
                normalizedSectors.map(
                    sector =>
                        sector.raw ||
                        null
                ),

            speed_trap:
                getF1AnalysisSpeedTrap(
                    line
                ),

            laps,

            stints:
                stints[number] ||
                [],

            long_run_average_seconds:
                longRunAverageSeconds,

            long_run_average:
                f1AnalysisFormatSeconds(
                    longRunAverageSeconds
                ),

            long_run_laps:
                longRunAverageSeconds
                    ? validLongRun.length
                    : 0
        });
    }

    drivers.sort((a,b) => {
        const pa =
            Number(a.position);
        const pb =
            Number(b.position);

        if (
            Number.isFinite(pa) &&
            Number.isFinite(pb)
        ) {
            return pa - pb;
        }

        const ba =
            f1AnalysisTimeToSeconds(
                a.best_lap
            );
        const bb =
            f1AnalysisTimeToSeconds(
                b.best_lap
            );

        if (
            Number.isFinite(ba) &&
            Number.isFinite(bb)
        ) {
            return ba - bb;
        }

        return 0;
    });

    const fallbackTotalLaps =
        Math.max(
            0,
            ...drivers.map(
                driver =>
                    Number(
                        driver.completed_laps ||
                        0
                    )
            )
        );

    const totalLaps =
        Number.isFinite(
            Number(
                lapCountState.total_laps
            )
        ) &&
        Number(
            lapCountState.total_laps
        ) > 0
            ? Number(
                lapCountState.total_laps
            )
            : fallbackTotalLaps;

    return {
        success:true,
        path:basePath,

        session:{
            name:
                sessionInfo?.Name ||
                sessionInfo?.Type ||
                null,

            type:
                sessionInfo?.Type ||
                null,

            start_date:
                sessionInfo?.StartDate ||
                null,

            end_date:
                sessionInfo?.EndDate ||
                null,

            meeting:
                sessionInfo?.Meeting?.Name ||
                null,

            circuit:
                sessionInfo?.Meeting?.Circuit?.ShortName ||
                sessionInfo?.Meeting?.Location ||
                null
        },

        total_laps:
            totalLaps,

        current_lap:
            lapCountState.current_lap,

        drivers,
        best_sectors:bestSectors,
        diagnostics:{
            timing_data_records:
                timingData.length,
            timing_data_f1_records:
                timingDataF1.length,
            timing_app_records:
                timingAppData.length,
            driver_records:
                driverRecords.length,
            lap_count_records:
                lapCountRecords.length,
            pit_lane_records:
                pitLaneRecords.length,
            tyre_stint_records:
                tyreStintRecords.length
        }
    };
}



app.get("/api/archive/replay-session", async (req, res) => {
    try {
        const path =
            String(req.query.path || "")
            .trim()
            .replace(/^\/+/, "");

        if (!path) {
            return res.status(400).json({
                ok:false,
                error:"Missing archive session path"
            });
        }

        /*
           Completed F1 sessions are available from the public static archive.
           Replay consumes the same timing topics instead of the live socket.
        */
        const base =
            "https://livetiming.formula1.com/static/" +
            path.replace(/\/+$/, "") +
            "/";

        const files = [
            "SessionInfo.json",
            "DriverList.json",
            "TimingData.jsonStream",
            "TimingAppData.jsonStream",
            "TimingStats.jsonStream",
            "SessionData.jsonStream",
            "SessionStatus.jsonStream",
            "ExtrapolatedClock.jsonStream",
            "TrackStatus.jsonStream",
            "WeatherData.jsonStream",
            "RaceControlMessages.jsonStream",
            "LapCount.jsonStream"
        ];

        const available = {};

        await Promise.all(
            files.map(async file => {
                try {
                    const response =
                        await fetch(base + file);

                    if (!response.ok) return;

                    available[file] =
                        await response.text();
                } catch {}
            })
        );

        res.json({
            ok:true,
            path,
            base,
            files:available
        });
    } catch (error) {
        res.status(500).json({
            ok:false,
            error:String(error?.message || error)
        });
    }
});


app.get("/api/archive/session-analysis", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        if (!sessionPath) {
            return res
                .status(400)
                .json({
                    success:false,
                    message:
                        "Ruta de sesión inválida."
                });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        const data =
            await buildF1ArchiveSessionAnalysis(
                basePath
            );

        res.set(
            "Cache-Control",
            "public, max-age=300"
        );

        res.json(
            data
        );

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success:false,
                message:
                    error.message ||
                    "No se pudo analizar la sesión."
            });
    }
});


app.get("/api/archive/session-stream", async (req, res) => {
    try {
        const sessionPath =
            validateArchivePath(
                req.query.path
            );

        const topicFile =
            getArchiveTopicFile(
                req.query.topic
            );

        if (!sessionPath || !topicFile) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Ruta de sesión o topic inválido."
                });
        }

        const basePath =
            sessionPath.endsWith("/")
                ? sessionPath
                : sessionPath + "/";

        const response =
            await fetchArchiveTextCached(
                basePath + topicFile
            );

        const compressed =
            isCompressedArchiveTopicFile(
                topicFile
            );

        const records =
            parseArchiveJsonStream(
                response.text,
                compressed
            );

        const limit =
            Math.max(
                1,
                Math.min(
                    Number(req.query.limit || 20),
                    200
                )
            );

        const tail =
            String(
                req.query.tail || "1"
            ) !== "0";

        const selected =
            tail
                ? records.slice(-limit)
                : records.slice(0, limit);

        res.set(
            "Cache-Control",
            "no-store"
        );

        res.json({
            success: true,
            path: basePath,
            topic:
                req.query.topic,
            file:
                topicFile,
            compressed,
            total_records:
                records.length,
            returned_records:
                selected.length,
            records:
                selected
        });

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});


app.get("/api/archive/:year", async (req, res) => {
    try {
        const year =
            validateYear(
                req.params.year
            );

        if (!year) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Año inválido."
                });
        }

        const result =
            await fetchArchiveText(
                `${year}/Index.json`
            );

        res.type("application/json");
        res.send(result.text);

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});

app.get("/api/archive/file", async (req, res) => {
    try {
        const path =
            validateArchivePath(
                req.query.path
            );

        if (!path) {
            return res
                .status(400)
                .json({
                    success: false,
                    message:
                        "Ruta inválida."
                });
        }

        const result =
            await fetchArchiveText(
                path
            );

        res.set(
            "Content-Type",
            result.contentType
        );

        res.set(
            "Cache-Control",
            "public, max-age=300"
        );

        res.send(result.text);

    } catch (error) {
        res
            .status(
                error.statusCode ||
                502
            )
            .json({
                success: false,
                message:
                    error.message
            });
    }
});

app.use((error, req, res, next) => {
    console.error(
        "[HTTP]",
        error.message
    );

    res
        .status(500)
        .json({
            success: false,
            message:
                "Error interno del Chavitoxo F1 Data Service."
        });
});


/* =========================================================
   ARRANQUE / APAGADO
========================================================= */

const server =
    app.listen(
        PORT,
        "0.0.0.0",
        () => {
            console.log(
                `Chavitoxo F1 Data Service V1.40.0 running on port ${PORT}`
            );

            console.log(
                "Allowed origins:",
                ALLOWED_ORIGINS.join(", ")
            );

            connectToF1();
        }
    );

function shutdown(signal) {
    console.log(
        `\n${signal}: shutting down...`
    );

    stopped = true;

    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }

    clearKeepAlive();
    clearConnectionWatchdog();

    if (sseBroadcastTimer) {
        clearTimeout(sseBroadcastTimer);
        sseBroadcastTimer = null;
    }

    if (f1Socket) {
        try {
            f1Socket.terminate();
        } catch {}
    }

    server.close(() => {
        process.exit(0);
    });

    setTimeout(() => {
        process.exit(1);
    }, 5000).unref();
}

process.on(
    "SIGINT",
    () => shutdown("SIGINT")
);

process.on(
    "SIGTERM",
    () => shutdown("SIGTERM")
);
