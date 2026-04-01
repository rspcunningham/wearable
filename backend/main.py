from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime
import sqlite3
import json
import os

app = FastAPI(title="Health Sync Server")

API_KEY = os.environ.get("HEALTH_API_KEY", "change-me-before-deploying")
api_key_header = APIKeyHeader(name="X-API-Key")

DB_PATH = "health.db"


# ── DB setup ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS health_records (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            record_type TEXT    NOT NULL,
            value       REAL,
            unit        TEXT,
            start_date  TEXT    NOT NULL,
            end_date    TEXT,
            device      TEXT,
            source_name TEXT,
            metadata    TEXT,
            received_at TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS workouts (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            workout_type          TEXT    NOT NULL,
            start_date            TEXT    NOT NULL,
            end_date              TEXT,
            duration_seconds      REAL,
            total_energy_burned   REAL,
            total_distance        REAL,
            source_name           TEXT,
            metadata              TEXT,
            received_at           TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS activity_summaries (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            date                        TEXT    NOT NULL,
            active_energy_burned        REAL,
            active_energy_burned_goal   REAL,
            apple_move_time             REAL,
            apple_move_time_goal        REAL,
            apple_exercise_time         REAL,
            apple_exercise_time_goal    REAL,
            apple_stand_hours           REAL,
            apple_stand_hours_goal      REAL,
            received_at                 TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS profile_snapshots (
            snapshot_name                TEXT    PRIMARY KEY,
            captured_at                  TEXT    NOT NULL,
            date_of_birth                TEXT,
            biological_sex_code          INTEGER,
            blood_type_code              INTEGER,
            fitzpatrick_skin_type_code   INTEGER,
            wheelchair_use_code          INTEGER,
            activity_move_mode_code      INTEGER,
            errors_json                  TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS electrocardiograms (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            number_of_voltage_measurements INTEGER NOT NULL,
            sampling_frequency_hz        REAL,
            classification_code          INTEGER NOT NULL,
            symptoms_status_code         INTEGER NOT NULL,
            average_heart_rate_bpm       REAL,
            voltage_measurements_json    TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS workout_routes (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            locations_json               TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS heartbeat_series (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            beats_json                   TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS audiograms (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            sensitivity_points_json      TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS state_of_mind_records (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            kind_code                    INTEGER NOT NULL,
            valence                      REAL    NOT NULL,
            valence_classification_code  INTEGER NOT NULL,
            label_codes_json             TEXT    NOT NULL,
            association_codes_json       TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS correlations (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_uuid                  TEXT    NOT NULL UNIQUE,
            correlation_type             TEXT    NOT NULL,
            start_date                   TEXT    NOT NULL,
            end_date                     TEXT,
            device                       TEXT,
            source_name                  TEXT,
            objects_json                 TEXT    NOT NULL,
            metadata                     TEXT,
            received_at                  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_records_type_date
            ON health_records(record_type, start_date);
        CREATE INDEX IF NOT EXISTS idx_workouts_date
            ON workouts(start_date);
        CREATE INDEX IF NOT EXISTS idx_ecg_start_date
            ON electrocardiograms(start_date);
        CREATE INDEX IF NOT EXISTS idx_workout_routes_start_date
            ON workout_routes(start_date);
        CREATE INDEX IF NOT EXISTS idx_heartbeat_series_start_date
            ON heartbeat_series(start_date);
        CREATE INDEX IF NOT EXISTS idx_audiograms_start_date
            ON audiograms(start_date);
        CREATE INDEX IF NOT EXISTS idx_state_of_mind_start_date
            ON state_of_mind_records(start_date);
        CREATE INDEX IF NOT EXISTS idx_correlations_start_date
            ON correlations(start_date);
    """)
    conn.commit()
    conn.close()


init_db()


def model_to_dict(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def dump_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


# ── Auth ──────────────────────────────────────────────────────────────────────

def verify_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


# ── Models ────────────────────────────────────────────────────────────────────

class HealthRecord(BaseModel):
    record_type: str
    value: Optional[float] = None
    unit: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class Workout(BaseModel):
    workout_type: str
    start_date: str
    end_date: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_energy_burned: Optional[float] = None
    total_distance: Optional[float] = None
    source_name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class ActivitySummary(BaseModel):
    date: str
    active_energy_burned: Optional[float] = None
    active_energy_burned_goal: Optional[float] = None
    apple_move_time: Optional[float] = None
    apple_move_time_goal: Optional[float] = None
    apple_exercise_time: Optional[float] = None
    apple_exercise_time_goal: Optional[float] = None
    apple_stand_hours: Optional[float] = None
    apple_stand_hours_goal: Optional[float] = None


class ProfileSnapshotRecord(BaseModel):
    captured_at: str
    date_of_birth: Optional[str] = None
    biological_sex_code: Optional[int] = None
    blood_type_code: Optional[int] = None
    fitzpatrick_skin_type_code: Optional[int] = None
    wheelchair_use_code: Optional[int] = None
    activity_move_mode_code: Optional[int] = None
    errors: Optional[dict[str, str]] = None


class ElectrocardiogramVoltageMeasurement(BaseModel):
    time_since_sample_start: float
    lead_values: dict[str, float] = Field(default_factory=dict)


class ElectrocardiogramRecord(BaseModel):
    sample_uuid: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    number_of_voltage_measurements: int
    sampling_frequency_hz: Optional[float] = None
    classification_code: int
    symptoms_status_code: int
    average_heart_rate_bpm: Optional[float] = None
    voltage_measurements: list[ElectrocardiogramVoltageMeasurement] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class WorkoutRouteLocation(BaseModel):
    timestamp: str
    latitude: float
    longitude: float
    altitude: float
    horizontal_accuracy: float
    vertical_accuracy: float
    course: float
    speed: float


class WorkoutRouteRecord(BaseModel):
    sample_uuid: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    locations: list[WorkoutRouteLocation] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class HeartbeatSeriesBeat(BaseModel):
    time_since_series_start: float
    preceded_by_gap: bool


class HeartbeatSeriesRecord(BaseModel):
    sample_uuid: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    beats: list[HeartbeatSeriesBeat] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class AudiogramSensitivityTest(BaseModel):
    sensitivity_dbhl: float
    conduction_type_code: int
    masked: bool
    side_code: int
    clamping_lower_bound_dbhl: Optional[float] = None
    clamping_upper_bound_dbhl: Optional[float] = None


class AudiogramSensitivityPoint(BaseModel):
    frequency_hz: float
    left_ear_sensitivity_dbhl: Optional[float] = None
    right_ear_sensitivity_dbhl: Optional[float] = None
    tests: list[AudiogramSensitivityTest] = Field(default_factory=list)


class AudiogramRecord(BaseModel):
    sample_uuid: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    sensitivity_points: list[AudiogramSensitivityPoint] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class StateOfMindRecord(BaseModel):
    sample_uuid: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    kind_code: int
    valence: float
    valence_classification_code: int
    label_codes: list[int] = Field(default_factory=list)
    association_codes: list[int] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class CorrelationObjectRecord(BaseModel):
    sample_uuid: str
    record_type: str
    value: Optional[float] = None
    unit: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    metadata: Optional[dict[str, str]] = None


class CorrelationRecord(BaseModel):
    sample_uuid: str
    correlation_type: str
    start_date: str
    end_date: Optional[str] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    objects: list[CorrelationObjectRecord] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class BatchPayload(BaseModel):
    records: list[HealthRecord] = Field(default_factory=list)
    workouts: list[Workout] = Field(default_factory=list)
    activity_summaries: list[ActivitySummary] = Field(default_factory=list)
    profile_snapshots: list[ProfileSnapshotRecord] = Field(default_factory=list)
    electrocardiograms: list[ElectrocardiogramRecord] = Field(default_factory=list)
    workout_routes: list[WorkoutRouteRecord] = Field(default_factory=list)
    heartbeat_series: list[HeartbeatSeriesRecord] = Field(default_factory=list)
    audiograms: list[AudiogramRecord] = Field(default_factory=list)
    state_of_mind: list[StateOfMindRecord] = Field(default_factory=list)
    correlations: list[CorrelationRecord] = Field(default_factory=list)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/health/batch", dependencies=[Depends(verify_api_key)])
def ingest_batch(payload: BatchPayload):
    """Main ingestion endpoint — iOS app posts batches here."""
    conn = get_db()
    records_inserted = 0
    workouts_inserted = 0
    summaries_inserted = 0
    profile_snapshots_inserted = 0
    electrocardiograms_inserted = 0
    workout_routes_inserted = 0
    heartbeat_series_inserted = 0
    audiograms_inserted = 0
    state_of_mind_inserted = 0
    correlations_inserted = 0

    for r in payload.records:
        conn.execute(
            """INSERT INTO health_records
               (record_type, value, unit, start_date, end_date, device, source_name, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (r.record_type, r.value, r.unit, r.start_date, r.end_date,
             r.device, r.source_name, json.dumps(r.metadata) if r.metadata else None)
        )
        records_inserted += 1

    for w in payload.workouts:
        conn.execute(
            """INSERT INTO workouts
               (workout_type, start_date, end_date, duration_seconds,
                total_energy_burned, total_distance, source_name, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (w.workout_type, w.start_date, w.end_date, w.duration_seconds,
             w.total_energy_burned, w.total_distance, w.source_name,
             json.dumps(w.metadata) if w.metadata else None)
        )
        workouts_inserted += 1

    for s in payload.activity_summaries:
        conn.execute(
            """INSERT INTO activity_summaries
               (date, active_energy_burned, active_energy_burned_goal,
                apple_move_time, apple_move_time_goal, apple_exercise_time,
                apple_exercise_time_goal, apple_stand_hours, apple_stand_hours_goal)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (s.date, s.active_energy_burned, s.active_energy_burned_goal,
             s.apple_move_time, s.apple_move_time_goal, s.apple_exercise_time,
             s.apple_exercise_time_goal, s.apple_stand_hours, s.apple_stand_hours_goal)
        )
        summaries_inserted += 1

    for snapshot in payload.profile_snapshots:
        conn.execute(
            """INSERT OR REPLACE INTO profile_snapshots
               (snapshot_name, captured_at, date_of_birth, biological_sex_code,
                blood_type_code, fitzpatrick_skin_type_code, wheelchair_use_code,
                activity_move_mode_code, errors_json, received_at)
               VALUES ('default', ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (
                snapshot.captured_at,
                snapshot.date_of_birth,
                snapshot.biological_sex_code,
                snapshot.blood_type_code,
                snapshot.fitzpatrick_skin_type_code,
                snapshot.wheelchair_use_code,
                snapshot.activity_move_mode_code,
                dump_json(snapshot.errors),
            )
        )
        profile_snapshots_inserted += 1

    for ecg in payload.electrocardiograms:
        conn.execute(
            """INSERT OR REPLACE INTO electrocardiograms
               (sample_uuid, start_date, end_date, device, source_name,
                number_of_voltage_measurements, sampling_frequency_hz,
                classification_code, symptoms_status_code, average_heart_rate_bpm,
                voltage_measurements_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ecg.sample_uuid, ecg.start_date, ecg.end_date, ecg.device, ecg.source_name,
                ecg.number_of_voltage_measurements, ecg.sampling_frequency_hz,
                ecg.classification_code, ecg.symptoms_status_code, ecg.average_heart_rate_bpm,
                dump_json([model_to_dict(m) for m in ecg.voltage_measurements]),
                dump_json(ecg.metadata),
            )
        )
        electrocardiograms_inserted += 1

    for route in payload.workout_routes:
        conn.execute(
            """INSERT OR REPLACE INTO workout_routes
               (sample_uuid, start_date, end_date, device, source_name, locations_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                route.sample_uuid, route.start_date, route.end_date, route.device, route.source_name,
                dump_json([model_to_dict(location) for location in route.locations]),
                dump_json(route.metadata),
            )
        )
        workout_routes_inserted += 1

    for series in payload.heartbeat_series:
        conn.execute(
            """INSERT OR REPLACE INTO heartbeat_series
               (sample_uuid, start_date, end_date, device, source_name, beats_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                series.sample_uuid, series.start_date, series.end_date, series.device, series.source_name,
                dump_json([model_to_dict(beat) for beat in series.beats]),
                dump_json(series.metadata),
            )
        )
        heartbeat_series_inserted += 1

    for audiogram in payload.audiograms:
        conn.execute(
            """INSERT OR REPLACE INTO audiograms
               (sample_uuid, start_date, end_date, device, source_name, sensitivity_points_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                audiogram.sample_uuid, audiogram.start_date, audiogram.end_date, audiogram.device, audiogram.source_name,
                dump_json([model_to_dict(point) for point in audiogram.sensitivity_points]),
                dump_json(audiogram.metadata),
            )
        )
        audiograms_inserted += 1

    for state in payload.state_of_mind:
        conn.execute(
            """INSERT OR REPLACE INTO state_of_mind_records
               (sample_uuid, start_date, end_date, device, source_name,
                kind_code, valence, valence_classification_code,
                label_codes_json, association_codes_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                state.sample_uuid, state.start_date, state.end_date, state.device, state.source_name,
                state.kind_code, state.valence, state.valence_classification_code,
                dump_json(state.label_codes), dump_json(state.association_codes),
                dump_json(state.metadata),
            )
        )
        state_of_mind_inserted += 1

    for correlation in payload.correlations:
        conn.execute(
            """INSERT OR REPLACE INTO correlations
               (sample_uuid, correlation_type, start_date, end_date, device, source_name, objects_json, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                correlation.sample_uuid, correlation.correlation_type, correlation.start_date, correlation.end_date,
                correlation.device, correlation.source_name,
                dump_json([model_to_dict(obj) for obj in correlation.objects]),
                dump_json(correlation.metadata),
            )
        )
        correlations_inserted += 1

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "inserted": {
            "records": records_inserted,
            "workouts": workouts_inserted,
            "activity_summaries": summaries_inserted,
            "profile_snapshots": profile_snapshots_inserted,
            "electrocardiograms": electrocardiograms_inserted,
            "workout_routes": workout_routes_inserted,
            "heartbeat_series": heartbeat_series_inserted,
            "audiograms": audiograms_inserted,
            "state_of_mind": state_of_mind_inserted,
            "correlations": correlations_inserted,
        }
    }


@app.get("/health/records", dependencies=[Depends(verify_api_key)])
def get_records(
    record_type: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 500
):
    conn = get_db()
    query = "SELECT * FROM health_records WHERE 1=1"
    params: list = []
    if record_type:
        query += " AND record_type = ?"
        params.append(record_type)
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/workouts", dependencies=[Depends(verify_api_key)])
def get_workouts(since: Optional[str] = None, limit: int = 200):
    conn = get_db()
    query = "SELECT * FROM workouts WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/electrocardiograms", dependencies=[Depends(verify_api_key)])
def get_electrocardiograms(since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM electrocardiograms WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/profile-snapshot", dependencies=[Depends(verify_api_key)])
def get_profile_snapshot():
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM profile_snapshots WHERE snapshot_name = 'default'"
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


@app.get("/health/workout-routes", dependencies=[Depends(verify_api_key)])
def get_workout_routes(since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM workout_routes WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/heartbeat-series", dependencies=[Depends(verify_api_key)])
def get_heartbeat_series(since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM heartbeat_series WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/audiograms", dependencies=[Depends(verify_api_key)])
def get_audiograms(since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM audiograms WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/state-of-mind", dependencies=[Depends(verify_api_key)])
def get_state_of_mind(since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM state_of_mind_records WHERE 1=1"
    params: list = []
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/correlations", dependencies=[Depends(verify_api_key)])
def get_correlations(correlation_type: Optional[str] = None, since: Optional[str] = None, limit: int = 100):
    conn = get_db()
    query = "SELECT * FROM correlations WHERE 1=1"
    params: list = []
    if correlation_type:
        query += " AND correlation_type = ?"
        params.append(correlation_type)
    if since:
        query += " AND start_date >= ?"
        params.append(since)
    query += " ORDER BY start_date DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/health/summary", dependencies=[Depends(verify_api_key)])
def get_summary():
    conn = get_db()
    record_types = conn.execute(
        "SELECT record_type, COUNT(*) as count FROM health_records GROUP BY record_type ORDER BY count DESC"
    ).fetchall()
    total_workouts = conn.execute("SELECT COUNT(*) FROM workouts").fetchone()[0]
    total_profile_snapshots = conn.execute("SELECT COUNT(*) FROM profile_snapshots").fetchone()[0]
    total_electrocardiograms = conn.execute("SELECT COUNT(*) FROM electrocardiograms").fetchone()[0]
    total_workout_routes = conn.execute("SELECT COUNT(*) FROM workout_routes").fetchone()[0]
    total_heartbeat_series = conn.execute("SELECT COUNT(*) FROM heartbeat_series").fetchone()[0]
    total_audiograms = conn.execute("SELECT COUNT(*) FROM audiograms").fetchone()[0]
    total_state_of_mind = conn.execute("SELECT COUNT(*) FROM state_of_mind_records").fetchone()[0]
    total_correlations = conn.execute("SELECT COUNT(*) FROM correlations").fetchone()[0]
    latest = conn.execute(
        """
        SELECT MAX(received_at) FROM (
            SELECT received_at FROM health_records
            UNION ALL SELECT received_at FROM workouts
            UNION ALL SELECT received_at FROM activity_summaries
            UNION ALL SELECT received_at FROM profile_snapshots
            UNION ALL SELECT received_at FROM electrocardiograms
            UNION ALL SELECT received_at FROM workout_routes
            UNION ALL SELECT received_at FROM heartbeat_series
            UNION ALL SELECT received_at FROM audiograms
            UNION ALL SELECT received_at FROM state_of_mind_records
            UNION ALL SELECT received_at FROM correlations
        )
        """
    ).fetchone()[0]
    conn.close()
    return {
        "record_types": [dict(r) for r in record_types],
        "total_workouts": total_workouts,
        "total_profile_snapshots": total_profile_snapshots,
        "total_electrocardiograms": total_electrocardiograms,
        "total_workout_routes": total_workout_routes,
        "total_heartbeat_series": total_heartbeat_series,
        "total_audiograms": total_audiograms,
        "total_state_of_mind": total_state_of_mind,
        "total_correlations": total_correlations,
        "latest_received_at": latest,
    }


@app.get("/ping")
def ping():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
