from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime, date
from dateutil.parser import isoparse
import json
import logging
import os
import time
import psycopg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("healthsync")

app = FastAPI(title="Health Sync Server")

API_KEY = os.environ.get("HEALTH_API_KEY", "change-me-before-deploying")
api_key_header = APIKeyHeader(name="X-API-Key")

DATABASE_URL = os.environ.get("DATABASE_URL")


# ── HealthKit enum mappings ──────────────────────────────────────────────────

HK_BIOLOGICAL_SEX = {
    0: "not_set", 1: "female", 2: "male", 3: "other",
}

HK_BLOOD_TYPE = {
    0: "not_set", 1: "a_positive", 2: "a_negative", 3: "b_positive",
    4: "b_negative", 5: "ab_positive", 6: "ab_negative", 7: "o_positive",
    8: "o_negative",
}

HK_FITZPATRICK_SKIN_TYPE = {
    0: "not_set", 1: "type_I", 2: "type_II", 3: "type_III",
    4: "type_IV", 5: "type_V", 6: "type_VI",
}

HK_WHEELCHAIR_USE = {
    0: "not_set", 1: "no", 2: "yes",
}

HK_ACTIVITY_MOVE_MODE = {
    0: "active_energy", 1: "apple_move_time",
}

HK_ECG_CLASSIFICATION = {
    0: "not_set", 1: "sinus_rhythm", 2: "atrial_fibrillation",
    3: "inconclusive_low_heart_rate", 4: "inconclusive_high_heart_rate",
    5: "inconclusive_poor_reading", 6: "inconclusive_other", 7: "unrecognized",
}

HK_SYMPTOMS_STATUS = {
    0: "not_set", 1: "none", 2: "present",
}

HK_STATE_OF_MIND_KIND = {
    1: "momentary_emotion", 2: "daily_mood",
}

HK_VALENCE_CLASSIFICATION = {
    1: "very_unpleasant", 2: "unpleasant", 3: "slightly_unpleasant",
    4: "neutral", 5: "slightly_pleasant", 6: "pleasant", 7: "very_pleasant",
}

HK_STATE_OF_MIND_LABEL = {
    1: "amazed", 2: "amused", 3: "angry", 4: "anxious", 5: "ashamed",
    6: "brave", 7: "calm", 8: "confident", 9: "content", 10: "disappointed",
    11: "discouraged", 12: "disgusted", 13: "drained", 14: "embarrassed",
    15: "excited", 16: "frustrated", 17: "grateful", 18: "guilty",
    19: "happy", 20: "hopeful", 21: "hopeless", 22: "indifferent",
    23: "irritated", 24: "jealous", 25: "joyful", 26: "lonely",
    27: "overwhelmed", 28: "peaceful", 29: "proud", 30: "relieved",
    31: "sad", 32: "scared", 33: "stressed", 34: "surprised",
    35: "worried",
}

HK_STATE_OF_MIND_ASSOCIATION = {
    1: "community", 2: "current_events", 3: "dating", 4: "education",
    5: "family", 6: "fitness", 7: "friends", 8: "health",
    9: "hobbies", 10: "identity", 11: "money", 12: "partner",
    13: "self_care", 14: "spirituality", 15: "tasks", 16: "travel",
    17: "weather", 18: "work",
}

HK_AUDIOGRAM_CONDUCTION_TYPE = {
    0: "air", 1: "bone",
}

HK_AUDIOGRAM_SIDE = {
    0: "left", 1: "right",
}


def resolve_code(mapping: dict[int, str], value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    return mapping.get(value, f"unknown_{value}")


def resolve_codes(mapping: dict[int, str], values: list[int]) -> list[str]:
    return [mapping.get(v, f"unknown_{v}") for v in values]


# ── DB setup ──────────────────────────────────────────────────────────────────

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required.")
    return psycopg.connect(DATABASE_URL)


def init_db():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS health_records (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            record_type TEXT NOT NULL,
            value DOUBLE PRECISION,
            unit TEXT,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS workouts (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            workout_type TEXT NOT NULL,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            duration_seconds DOUBLE PRECISION,
            total_energy_burned DOUBLE PRECISION,
            total_distance DOUBLE PRECISION,
            source_name TEXT,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS activity_summaries (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            active_energy_burned DOUBLE PRECISION,
            active_energy_burned_goal DOUBLE PRECISION,
            apple_move_time DOUBLE PRECISION,
            apple_move_time_goal DOUBLE PRECISION,
            apple_exercise_time DOUBLE PRECISION,
            apple_exercise_time_goal DOUBLE PRECISION,
            apple_stand_hours DOUBLE PRECISION,
            apple_stand_hours_goal DOUBLE PRECISION,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS profile_snapshots (
            snapshot_name TEXT PRIMARY KEY,
            captured_at TIMESTAMPTZ NOT NULL,
            date_of_birth DATE,
            biological_sex TEXT,
            blood_type TEXT,
            fitzpatrick_skin_type TEXT,
            wheelchair_use TEXT,
            activity_move_mode TEXT,
            errors_json TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS electrocardiograms (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            number_of_voltage_measurements INTEGER NOT NULL,
            sampling_frequency_hz DOUBLE PRECISION,
            classification TEXT NOT NULL,
            symptoms_status TEXT NOT NULL,
            average_heart_rate_bpm DOUBLE PRECISION,
            voltage_measurements_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS workout_routes (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            locations_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS heartbeat_series (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            beats_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS audiograms (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            sensitivity_points_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS state_of_mind_records (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            kind TEXT NOT NULL,
            valence DOUBLE PRECISION NOT NULL,
            valence_classification TEXT NOT NULL,
            labels_json TEXT NOT NULL,
            associations_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS correlations (
            id BIGSERIAL PRIMARY KEY,
            sample_uuid TEXT NOT NULL UNIQUE,
            correlation_type TEXT NOT NULL,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            device TEXT,
            source_name TEXT,
            objects_json TEXT NOT NULL,
            metadata TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ,
            update_count INTEGER NOT NULL DEFAULT 0
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_records_type_date ON health_records(record_type, start_date)",
        "CREATE INDEX IF NOT EXISTS idx_workouts_date ON workouts(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_ecg_start_date ON electrocardiograms(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_workout_routes_start_date ON workout_routes(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_heartbeat_series_start_date ON heartbeat_series(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_audiograms_start_date ON audiograms(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_state_of_mind_start_date ON state_of_mind_records(start_date)",
        "CREATE INDEX IF NOT EXISTS idx_correlations_start_date ON correlations(start_date)",
    ]

    with get_db() as conn:
        for statement in statements:
            conn.execute(statement)


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
    sample_uuid: str
    record_type: str
    value: Optional[float] = None
    unit: Optional[str] = None
    start_date: datetime
    end_date: Optional[datetime] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class Workout(BaseModel):
    sample_uuid: str
    workout_type: str
    start_date: datetime
    end_date: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    total_energy_burned: Optional[float] = None
    total_distance: Optional[float] = None
    source_name: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class ActivitySummary(BaseModel):
    date: str  # accepts ISO-8601 datetime or YYYY-MM-DD; normalized to date at insert
    active_energy_burned: Optional[float] = None
    active_energy_burned_goal: Optional[float] = None
    apple_move_time: Optional[float] = None
    apple_move_time_goal: Optional[float] = None
    apple_exercise_time: Optional[float] = None
    apple_exercise_time_goal: Optional[float] = None
    apple_stand_hours: Optional[float] = None
    apple_stand_hours_goal: Optional[float] = None


class ProfileSnapshotRecord(BaseModel):
    captured_at: datetime
    date_of_birth: Optional[str] = None  # "YYYY-MM-DD" or None
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
    start_date: datetime
    end_date: Optional[datetime] = None
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
    start_date: datetime
    end_date: Optional[datetime] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    locations: list[WorkoutRouteLocation] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class HeartbeatSeriesBeat(BaseModel):
    time_since_series_start: float
    preceded_by_gap: bool


class HeartbeatSeriesRecord(BaseModel):
    sample_uuid: str
    start_date: datetime
    end_date: Optional[datetime] = None
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
    start_date: datetime
    end_date: Optional[datetime] = None
    device: Optional[str] = None
    source_name: Optional[str] = None
    sensitivity_points: list[AudiogramSensitivityPoint] = Field(default_factory=list)
    metadata: Optional[dict[str, str]] = None


class StateOfMindRecord(BaseModel):
    sample_uuid: str
    start_date: datetime
    end_date: Optional[datetime] = None
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
    start_date: datetime
    end_date: Optional[datetime] = None
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


# ── Audiogram JSON resolution ────────────────────────────────────────────────

def resolve_audiogram_point(point: AudiogramSensitivityPoint) -> dict:
    d = model_to_dict(point)
    d["tests"] = [
        {
            "sensitivity_dbhl": t["sensitivity_dbhl"],
            "conduction_type": resolve_code(HK_AUDIOGRAM_CONDUCTION_TYPE, t["conduction_type_code"]),
            "masked": t["masked"],
            "side": resolve_code(HK_AUDIOGRAM_SIDE, t["side_code"]),
            "clamping_lower_bound_dbhl": t["clamping_lower_bound_dbhl"],
            "clamping_upper_bound_dbhl": t["clamping_upper_bound_dbhl"],
        }
        for t in d["tests"]
    ]
    return d


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/ingest", dependencies=[Depends(verify_api_key)])
def ingest_batch(payload: BatchPayload):
    """Main ingestion endpoint — iOS app posts batches here."""
    counts = {
        "records": len(payload.records),
        "workouts": len(payload.workouts),
        "activity_summaries": len(payload.activity_summaries),
        "profile_snapshots": len(payload.profile_snapshots),
        "electrocardiograms": len(payload.electrocardiograms),
        "workout_routes": len(payload.workout_routes),
        "heartbeat_series": len(payload.heartbeat_series),
        "audiograms": len(payload.audiograms),
        "state_of_mind": len(payload.state_of_mind),
        "correlations": len(payload.correlations),
    }
    nonempty = {k: v for k, v in counts.items() if v > 0}
    total = sum(counts.values())
    logger.info(f"INGEST: {total} items — {nonempty}")
    t0 = time.time()

    try:
        _do_ingest(payload)
    except Exception:
        logger.exception("INGEST FAILED")
        raise

    elapsed = time.time() - t0
    logger.info(f"INGEST OK: {total} items in {elapsed:.2f}s")
    return {"status": "ok", "inserted": counts}


def _do_ingest(payload: BatchPayload):
    with get_db() as conn:
        cur = conn.cursor()
        if payload.records:
            cur.executemany(
                """
                INSERT INTO health_records
                    (sample_uuid, record_type, value, unit, start_date, end_date, device, source_name, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    record_type = EXCLUDED.record_type,
                    value = EXCLUDED.value,
                    unit = EXCLUDED.unit,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = health_records.update_count + 1
                """,
                [
                    (r.sample_uuid, r.record_type, r.value, r.unit, r.start_date, r.end_date,
                     r.device, r.source_name, dump_json(r.metadata))
                    for r in payload.records
                ],
            )

        if payload.workouts:
            cur.executemany(
                """
                INSERT INTO workouts
                    (sample_uuid, workout_type, start_date, end_date, duration_seconds,
                     total_energy_burned, total_distance, source_name, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    workout_type = EXCLUDED.workout_type,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    duration_seconds = EXCLUDED.duration_seconds,
                    total_energy_burned = EXCLUDED.total_energy_burned,
                    total_distance = EXCLUDED.total_distance,
                    source_name = EXCLUDED.source_name,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = workouts.update_count + 1
                """,
                [
                    (w.sample_uuid, w.workout_type, w.start_date, w.end_date, w.duration_seconds,
                     w.total_energy_burned, w.total_distance, w.source_name, dump_json(w.metadata))
                    for w in payload.workouts
                ],
            )

        if payload.activity_summaries:
            cur.executemany(
                """
                INSERT INTO activity_summaries
                    (date, active_energy_burned, active_energy_burned_goal,
                     apple_move_time, apple_move_time_goal, apple_exercise_time,
                     apple_exercise_time_goal, apple_stand_hours, apple_stand_hours_goal)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    active_energy_burned = EXCLUDED.active_energy_burned,
                    active_energy_burned_goal = EXCLUDED.active_energy_burned_goal,
                    apple_move_time = EXCLUDED.apple_move_time,
                    apple_move_time_goal = EXCLUDED.apple_move_time_goal,
                    apple_exercise_time = EXCLUDED.apple_exercise_time,
                    apple_exercise_time_goal = EXCLUDED.apple_exercise_time_goal,
                    apple_stand_hours = EXCLUDED.apple_stand_hours,
                    apple_stand_hours_goal = EXCLUDED.apple_stand_hours_goal,
                    updated_at = NOW(),
                    update_count = activity_summaries.update_count + 1
                """,
                [
                    (isoparse(s.date).date().isoformat(), s.active_energy_burned, s.active_energy_burned_goal,
                     s.apple_move_time, s.apple_move_time_goal, s.apple_exercise_time,
                     s.apple_exercise_time_goal, s.apple_stand_hours, s.apple_stand_hours_goal)
                    for s in payload.activity_summaries
                ],
            )

        if payload.profile_snapshots:
            cur.executemany(
                """
                INSERT INTO profile_snapshots
                    (snapshot_name, captured_at, date_of_birth, biological_sex,
                     blood_type, fitzpatrick_skin_type, wheelchair_use,
                     activity_move_mode, errors_json)
                VALUES ('default', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_name) DO UPDATE SET
                    captured_at = EXCLUDED.captured_at,
                    date_of_birth = EXCLUDED.date_of_birth,
                    biological_sex = EXCLUDED.biological_sex,
                    blood_type = EXCLUDED.blood_type,
                    fitzpatrick_skin_type = EXCLUDED.fitzpatrick_skin_type,
                    wheelchair_use = EXCLUDED.wheelchair_use,
                    activity_move_mode = EXCLUDED.activity_move_mode,
                    errors_json = EXCLUDED.errors_json,
                    updated_at = NOW(),
                    update_count = profile_snapshots.update_count + 1
                """,
                [
                    (snapshot.captured_at, snapshot.date_of_birth,
                     resolve_code(HK_BIOLOGICAL_SEX, snapshot.biological_sex_code),
                     resolve_code(HK_BLOOD_TYPE, snapshot.blood_type_code),
                     resolve_code(HK_FITZPATRICK_SKIN_TYPE, snapshot.fitzpatrick_skin_type_code),
                     resolve_code(HK_WHEELCHAIR_USE, snapshot.wheelchair_use_code),
                     resolve_code(HK_ACTIVITY_MOVE_MODE, snapshot.activity_move_mode_code),
                     dump_json(snapshot.errors))
                    for snapshot in payload.profile_snapshots
                ],
            )

        if payload.electrocardiograms:
            cur.executemany(
                """
                INSERT INTO electrocardiograms
                    (sample_uuid, start_date, end_date, device, source_name,
                     number_of_voltage_measurements, sampling_frequency_hz,
                     classification, symptoms_status, average_heart_rate_bpm,
                     voltage_measurements_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    number_of_voltage_measurements = EXCLUDED.number_of_voltage_measurements,
                    sampling_frequency_hz = EXCLUDED.sampling_frequency_hz,
                    classification = EXCLUDED.classification,
                    symptoms_status = EXCLUDED.symptoms_status,
                    average_heart_rate_bpm = EXCLUDED.average_heart_rate_bpm,
                    voltage_measurements_json = EXCLUDED.voltage_measurements_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = electrocardiograms.update_count + 1
                """,
                [
                    (ecg.sample_uuid, ecg.start_date, ecg.end_date, ecg.device, ecg.source_name,
                     ecg.number_of_voltage_measurements, ecg.sampling_frequency_hz,
                     resolve_code(HK_ECG_CLASSIFICATION, ecg.classification_code),
                     resolve_code(HK_SYMPTOMS_STATUS, ecg.symptoms_status_code),
                     ecg.average_heart_rate_bpm,
                     dump_json([model_to_dict(m) for m in ecg.voltage_measurements]),
                     dump_json(ecg.metadata))
                    for ecg in payload.electrocardiograms
                ],
            )

        if payload.workout_routes:
            cur.executemany(
                """
                INSERT INTO workout_routes
                    (sample_uuid, start_date, end_date, device, source_name, locations_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    locations_json = EXCLUDED.locations_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = workout_routes.update_count + 1
                """,
                [
                    (route.sample_uuid, route.start_date, route.end_date, route.device, route.source_name,
                     dump_json([model_to_dict(loc) for loc in route.locations]),
                     dump_json(route.metadata))
                    for route in payload.workout_routes
                ],
            )

        if payload.heartbeat_series:
            cur.executemany(
                """
                INSERT INTO heartbeat_series
                    (sample_uuid, start_date, end_date, device, source_name, beats_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    beats_json = EXCLUDED.beats_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = heartbeat_series.update_count + 1
                """,
                [
                    (series.sample_uuid, series.start_date, series.end_date, series.device, series.source_name,
                     dump_json([model_to_dict(beat) for beat in series.beats]),
                     dump_json(series.metadata))
                    for series in payload.heartbeat_series
                ],
            )

        if payload.audiograms:
            cur.executemany(
                """
                INSERT INTO audiograms
                    (sample_uuid, start_date, end_date, device, source_name, sensitivity_points_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    sensitivity_points_json = EXCLUDED.sensitivity_points_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = audiograms.update_count + 1
                """,
                [
                    (ag.sample_uuid, ag.start_date, ag.end_date, ag.device, ag.source_name,
                     dump_json([resolve_audiogram_point(pt) for pt in ag.sensitivity_points]),
                     dump_json(ag.metadata))
                    for ag in payload.audiograms
                ],
            )

        if payload.state_of_mind:
            cur.executemany(
                """
                INSERT INTO state_of_mind_records
                    (sample_uuid, start_date, end_date, device, source_name,
                     kind, valence, valence_classification,
                     labels_json, associations_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    kind = EXCLUDED.kind,
                    valence = EXCLUDED.valence,
                    valence_classification = EXCLUDED.valence_classification,
                    labels_json = EXCLUDED.labels_json,
                    associations_json = EXCLUDED.associations_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = state_of_mind_records.update_count + 1
                """,
                [
                    (s.sample_uuid, s.start_date, s.end_date, s.device, s.source_name,
                     resolve_code(HK_STATE_OF_MIND_KIND, s.kind_code),
                     s.valence,
                     resolve_code(HK_VALENCE_CLASSIFICATION, s.valence_classification_code),
                     dump_json(resolve_codes(HK_STATE_OF_MIND_LABEL, s.label_codes)),
                     dump_json(resolve_codes(HK_STATE_OF_MIND_ASSOCIATION, s.association_codes)),
                     dump_json(s.metadata))
                    for s in payload.state_of_mind
                ],
            )

        if payload.correlations:
            cur.executemany(
                """
                INSERT INTO correlations
                    (sample_uuid, correlation_type, start_date, end_date, device, source_name, objects_json, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_uuid) DO UPDATE SET
                    correlation_type = EXCLUDED.correlation_type,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    device = EXCLUDED.device,
                    source_name = EXCLUDED.source_name,
                    objects_json = EXCLUDED.objects_json,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW(),
                    update_count = correlations.update_count + 1
                """,
                [
                    (c.sample_uuid, c.correlation_type, c.start_date, c.end_date,
                     c.device, c.source_name,
                     dump_json([model_to_dict(obj) for obj in c.objects]),
                     dump_json(c.metadata))
                    for c in payload.correlations
                ],
            )



@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
