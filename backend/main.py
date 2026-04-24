from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime, date, timezone
from dateutil.parser import isoparse
from pathlib import Path
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
import uuid

import duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("healthsync")

app = FastAPI(title="Health Sync Server")

API_KEY = os.environ.get("HEALTH_API_KEY", "change-me-before-deploying")
api_key_header = APIKeyHeader(name="X-API-Key")

DEFAULT_DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "./data")
DATA_DIR = Path(os.environ.get("HEALTHSYNC_DATA_DIR", DEFAULT_DATA_DIR))
SQLITE_PATH = Path(os.environ.get("SQLITE_PATH", str(DATA_DIR / "ingest.sqlite")))
PARQUET_ROOT = Path(os.environ.get("PARQUET_ROOT", str(DATA_DIR / "parquet")))


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


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return to_utc(value).isoformat()


def duration_seconds(start: datetime, end: Optional[datetime]) -> Optional[float]:
    if end is None:
        return None
    return (to_utc(end) - to_utc(start)).total_seconds()


def model_to_dict(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def dump_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


# ── Auth ──────────────────────────────────────────────────────────────────────

def verify_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


# ── Request models ────────────────────────────────────────────────────────────

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
    date: str
    active_energy_burned: Optional[float] = None
    active_energy_burned_goal: Optional[float] = None
    apple_move_time: Optional[float] = None
    apple_move_time_goal: Optional[float] = None
    apple_exercise_time: Optional[float] = None
    apple_exercise_time_goal: Optional[float] = None
    apple_stand_hours: Optional[float] = None
    apple_stand_hours_goal: Optional[float] = None


class RegisterPayload(BaseModel):
    name: str
    date_of_birth: Optional[str] = None
    biological_sex_code: Optional[int] = None
    blood_type_code: Optional[int] = None
    fitzpatrick_skin_type_code: Optional[int] = None
    wheelchair_use_code: Optional[int] = None
    activity_move_mode_code: Optional[int] = None


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
    electrocardiograms: list[ElectrocardiogramRecord] = Field(default_factory=list)
    workout_routes: list[WorkoutRouteRecord] = Field(default_factory=list)
    heartbeat_series: list[HeartbeatSeriesRecord] = Field(default_factory=list)
    audiograms: list[AudiogramRecord] = Field(default_factory=list)
    state_of_mind: list[StateOfMindRecord] = Field(default_factory=list)
    correlations: list[CorrelationRecord] = Field(default_factory=list)


class CanonicalEvent(BaseModel):
    event_id: str
    source_collection: str
    event_kind: str
    sample_uuid: Optional[str] = None
    record_type: Optional[str] = None
    start_ts: str
    end_ts: Optional[str] = None
    event_date: str
    duration_seconds: Optional[float] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    source_name: Optional[str] = None
    device: Optional[str] = None
    metadata_json: Optional[str] = None
    payload_json: str
    received_at: str


# ── Canonical event conversion ────────────────────────────────────────────────

def make_event(
    *,
    event_id: str,
    source_collection: str,
    event_kind: str,
    start: datetime,
    end: Optional[datetime],
    payload: dict[str, Any],
    received_at: datetime,
    sample_uuid: Optional[str] = None,
    record_type: Optional[str] = None,
    value: Optional[float] = None,
    unit: Optional[str] = None,
    source_name: Optional[str] = None,
    device: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> CanonicalEvent:
    start_utc = to_utc(start)
    end_utc = to_utc(end) if end else None
    return CanonicalEvent(
        event_id=event_id,
        source_collection=source_collection,
        event_kind=event_kind,
        sample_uuid=sample_uuid,
        record_type=record_type,
        start_ts=start_utc.isoformat(),
        end_ts=end_utc.isoformat() if end_utc else None,
        event_date=start_utc.date().isoformat(),
        duration_seconds=duration_seconds(start_utc, end_utc),
        value=value,
        unit=unit,
        source_name=source_name,
        device=device,
        metadata_json=dump_json(metadata),
        payload_json=dump_json(payload) or "{}",
        received_at=iso_utc(received_at),
    )


def activity_summary_date(value: str) -> date:
    try:
        return isoparse(value).date()
    except ValueError:
        return date.fromisoformat(value)


def canonicalize_payload(payload: BatchPayload, received_at: Optional[datetime] = None) -> list[CanonicalEvent]:
    received_at = received_at or utc_now()
    events: list[CanonicalEvent] = []

    for record in payload.records:
        d = model_to_dict(record)
        events.append(make_event(
            event_id=f"health_record:{record.sample_uuid}",
            source_collection="records",
            event_kind="health_record",
            sample_uuid=record.sample_uuid,
            record_type=record.record_type,
            start=record.start_date,
            end=record.end_date,
            value=record.value,
            unit=record.unit,
            source_name=record.source_name,
            device=record.device,
            metadata=record.metadata,
            payload=d,
            received_at=received_at,
        ))

    for workout in payload.workouts:
        d = model_to_dict(workout)
        events.append(make_event(
            event_id=f"workout:{workout.sample_uuid}",
            source_collection="workouts",
            event_kind="workout",
            sample_uuid=workout.sample_uuid,
            record_type=workout.workout_type,
            start=workout.start_date,
            end=workout.end_date,
            value=workout.duration_seconds,
            unit="s" if workout.duration_seconds is not None else None,
            source_name=workout.source_name,
            metadata=workout.metadata,
            payload=d,
            received_at=received_at,
        ))

    for summary in payload.activity_summaries:
        d = model_to_dict(summary)
        summary_day = activity_summary_date(summary.date)
        summary_start = datetime.combine(summary_day, datetime.min.time(), tzinfo=timezone.utc)
        events.append(make_event(
            event_id=f"activity_summary:{summary_day.isoformat()}",
            source_collection="activity_summaries",
            event_kind="activity_summary",
            record_type="activity_summary",
            start=summary_start,
            end=None,
            payload=d,
            received_at=received_at,
        ))

    for ecg in payload.electrocardiograms:
        d = model_to_dict(ecg)
        d["classification"] = resolve_code(HK_ECG_CLASSIFICATION, ecg.classification_code)
        d["symptoms_status"] = resolve_code(HK_SYMPTOMS_STATUS, ecg.symptoms_status_code)
        events.append(make_event(
            event_id=f"electrocardiogram:{ecg.sample_uuid}",
            source_collection="electrocardiograms",
            event_kind="electrocardiogram",
            sample_uuid=ecg.sample_uuid,
            record_type="electrocardiogram",
            start=ecg.start_date,
            end=ecg.end_date,
            source_name=ecg.source_name,
            device=ecg.device,
            metadata=ecg.metadata,
            payload=d,
            received_at=received_at,
        ))

    for route in payload.workout_routes:
        d = model_to_dict(route)
        events.append(make_event(
            event_id=f"workout_route:{route.sample_uuid}",
            source_collection="workout_routes",
            event_kind="workout_route",
            sample_uuid=route.sample_uuid,
            record_type="workout_route",
            start=route.start_date,
            end=route.end_date,
            source_name=route.source_name,
            device=route.device,
            metadata=route.metadata,
            payload=d,
            received_at=received_at,
        ))

    for series in payload.heartbeat_series:
        d = model_to_dict(series)
        events.append(make_event(
            event_id=f"heartbeat_series:{series.sample_uuid}",
            source_collection="heartbeat_series",
            event_kind="heartbeat_series",
            sample_uuid=series.sample_uuid,
            record_type="heartbeat_series",
            start=series.start_date,
            end=series.end_date,
            source_name=series.source_name,
            device=series.device,
            metadata=series.metadata,
            payload=d,
            received_at=received_at,
        ))

    for audiogram in payload.audiograms:
        d = model_to_dict(audiogram)
        d["sensitivity_points"] = [resolve_audiogram_point(point) for point in audiogram.sensitivity_points]
        events.append(make_event(
            event_id=f"audiogram:{audiogram.sample_uuid}",
            source_collection="audiograms",
            event_kind="audiogram",
            sample_uuid=audiogram.sample_uuid,
            record_type="audiogram",
            start=audiogram.start_date,
            end=audiogram.end_date,
            source_name=audiogram.source_name,
            device=audiogram.device,
            metadata=audiogram.metadata,
            payload=d,
            received_at=received_at,
        ))

    for state in payload.state_of_mind:
        d = model_to_dict(state)
        d["kind"] = resolve_code(HK_STATE_OF_MIND_KIND, state.kind_code)
        d["valence_classification"] = resolve_code(HK_VALENCE_CLASSIFICATION, state.valence_classification_code)
        d["labels"] = resolve_codes(HK_STATE_OF_MIND_LABEL, state.label_codes)
        d["associations"] = resolve_codes(HK_STATE_OF_MIND_ASSOCIATION, state.association_codes)
        events.append(make_event(
            event_id=f"state_of_mind:{state.sample_uuid}",
            source_collection="state_of_mind",
            event_kind="state_of_mind",
            sample_uuid=state.sample_uuid,
            record_type=d["kind"],
            start=state.start_date,
            end=state.end_date,
            value=state.valence,
            source_name=state.source_name,
            device=state.device,
            metadata=state.metadata,
            payload=d,
            received_at=received_at,
        ))

    for correlation in payload.correlations:
        d = model_to_dict(correlation)
        events.append(make_event(
            event_id=f"correlation:{correlation.sample_uuid}",
            source_collection="correlations",
            event_kind="correlation",
            sample_uuid=correlation.sample_uuid,
            record_type=correlation.correlation_type,
            start=correlation.start_date,
            end=correlation.end_date,
            source_name=correlation.source_name,
            device=correlation.device,
            metadata=correlation.metadata,
            payload=d,
            received_at=received_at,
        ))

    return events


# ── Audiogram JSON resolution ────────────────────────────────────────────────

def resolve_audiogram_point(point: AudiogramSensitivityPoint) -> dict[str, Any]:
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


# ── SQLite + Parquet storage ─────────────────────────────────────────────────

CANONICAL_COLUMNS = [
    "event_id", "source_collection", "event_kind", "sample_uuid", "record_type",
    "start_ts", "end_ts", "event_date", "duration_seconds", "value", "unit",
    "source_name", "device", "metadata_json", "payload_json", "received_at",
]


class HealthStorage:
    def __init__(
        self,
        *,
        data_dir: Path = DATA_DIR,
        sqlite_path: Path = SQLITE_PATH,
        parquet_root: Path = PARQUET_ROOT,
    ):
        self.data_dir = Path(data_dir)
        self.sqlite_path = Path(sqlite_path)
        self.parquet_root = Path(parquet_root)
        self.flush_lock = threading.Lock()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_root.mkdir(parents=True, exist_ok=True)
        (self.parquet_root / ".tmp").mkdir(parents=True, exist_ok=True)
        self.init_db()
        self.recover_unfinished_flushes()

    @classmethod
    def from_env(cls) -> "HealthStorage":
        return cls(
            data_dir=DATA_DIR,
            sqlite_path=SQLITE_PATH,
            parquet_root=PARQUET_ROOT,
        )

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingest_events (
                  event_id TEXT PRIMARY KEY,
                  source_collection TEXT NOT NULL,
                  event_kind TEXT NOT NULL,
                  sample_uuid TEXT,
                  record_type TEXT,
                  start_ts TEXT NOT NULL,
                  end_ts TEXT,
                  event_date TEXT NOT NULL,
                  duration_seconds REAL,
                  value REAL,
                  unit TEXT,
                  source_name TEXT,
                  device TEXT,
                  metadata_json TEXT,
                  payload_json TEXT NOT NULL,
                  received_at TEXT NOT NULL,
                  flush_id TEXT
                );

                CREATE TABLE IF NOT EXISTS seen_events (
                  event_id TEXT PRIMARY KEY,
                  source_collection TEXT NOT NULL,
                  event_date TEXT NOT NULL,
                  first_received_at TEXT NOT NULL,
                  last_received_at TEXT NOT NULL,
                  flushed_at TEXT,
                  flush_id TEXT,
                  update_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS flush_runs (
                  flush_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  completed_at TEXT,
                  row_count INTEGER NOT NULL DEFAULT 0,
                  error TEXT
                );

                CREATE TABLE IF NOT EXISTS parquet_files (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  flush_id TEXT NOT NULL,
                  event_date TEXT NOT NULL,
                  path TEXT NOT NULL UNIQUE,
                  row_count INTEGER NOT NULL,
                  min_start_ts TEXT,
                  max_start_ts TEXT,
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_profiles (
                  id INTEGER PRIMARY KEY CHECK (id = 1),
                  payload_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS storage_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_ingest_events_flush_date
                  ON ingest_events(flush_id, event_date, start_ts);
                CREATE INDEX IF NOT EXISTS idx_seen_events_date
                  ON seen_events(event_date);
                CREATE INDEX IF NOT EXISTS idx_parquet_files_date
                  ON parquet_files(event_date);
                CREATE INDEX IF NOT EXISTS idx_flush_runs_status
                  ON flush_runs(status, started_at);
                """
            )

    def recover_unfinished_flushes(self) -> None:
        tmp_dir = self.parquet_root / ".tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        now = iso_utc(utc_now())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT flush_id FROM flush_runs WHERE status = 'writing'"
                ).fetchall()
                for row in rows:
                    flush_id = row["flush_id"]
                    conn.execute(
                        "UPDATE ingest_events SET flush_id = NULL WHERE flush_id = ?",
                        (flush_id,),
                    )
                    conn.execute(
                        "UPDATE flush_runs SET status = 'failed', completed_at = ?, error = ? WHERE flush_id = ?",
                        (now, "Recovered after interrupted write", flush_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def store_events(self, events: list[CanonicalEvent]) -> int:
        if not events:
            return 0

        accepted = 0
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for event in events:
                    row = event.model_dump()
                    seen = conn.execute(
                        "SELECT flushed_at FROM seen_events WHERE event_id = ?",
                        (event.event_id,),
                    ).fetchone()

                    if seen and seen["flushed_at"] is not None:
                        conn.execute(
                            """
                            UPDATE seen_events
                            SET last_received_at = ?, update_count = update_count + 1
                            WHERE event_id = ?
                            """,
                            (event.received_at, event.event_id),
                        )
                        accepted += 1
                        continue

                    if seen:
                        conn.execute(
                            """
                            UPDATE seen_events
                            SET last_received_at = ?, update_count = update_count + 1
                            WHERE event_id = ?
                            """,
                            (event.received_at, event.event_id),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO seen_events
                              (event_id, source_collection, event_date, first_received_at, last_received_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (event.event_id, event.source_collection, event.event_date, event.received_at, event.received_at),
                        )

                    conn.execute(
                        f"""
                        INSERT INTO ingest_events ({', '.join(CANONICAL_COLUMNS)})
                        VALUES ({', '.join(['?'] * len(CANONICAL_COLUMNS))})
                        ON CONFLICT(event_id) DO UPDATE SET
                          source_collection = excluded.source_collection,
                          event_kind = excluded.event_kind,
                          sample_uuid = excluded.sample_uuid,
                          record_type = excluded.record_type,
                          start_ts = excluded.start_ts,
                          end_ts = excluded.end_ts,
                          event_date = excluded.event_date,
                          duration_seconds = excluded.duration_seconds,
                          value = excluded.value,
                          unit = excluded.unit,
                          source_name = excluded.source_name,
                          device = excluded.device,
                          metadata_json = excluded.metadata_json,
                          payload_json = excluded.payload_json,
                          received_at = excluded.received_at,
                          flush_id = NULL
                        """,
                        tuple(row[column] for column in CANONICAL_COLUMNS),
                    )
                    accepted += 1
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return accepted

    def store_user_profile(self, payload: RegisterPayload) -> None:
        profile = model_to_dict(payload)
        profile["biological_sex"] = resolve_code(HK_BIOLOGICAL_SEX, payload.biological_sex_code)
        profile["blood_type"] = resolve_code(HK_BLOOD_TYPE, payload.blood_type_code)
        profile["fitzpatrick_skin_type"] = resolve_code(HK_FITZPATRICK_SKIN_TYPE, payload.fitzpatrick_skin_type_code)
        profile["wheelchair_use"] = resolve_code(HK_WHEELCHAIR_USE, payload.wheelchair_use_code)
        profile["activity_move_mode"] = resolve_code(HK_ACTIVITY_MOVE_MODE, payload.activity_move_mode_code)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO user_profiles (id, payload_json, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (dump_json(profile), iso_utc(utc_now())),
            )

    def pending_count(self) -> int:
        with self.connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM ingest_events WHERE flush_id IS NULL").fetchone()[0])

    def finalizable_event_dates(self, cutoff_date: Optional[date] = None) -> list[str]:
        cutoff = (cutoff_date or utc_now().date()).isoformat()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT event_date
                FROM ingest_events
                WHERE flush_id IS NULL AND event_date < ?
                ORDER BY event_date
                """,
                (cutoff,),
            ).fetchall()
        return [row["event_date"] for row in rows]

    def last_completed_flush_at(self) -> Optional[datetime]:
        with self.connect() as conn:
            value = conn.execute(
                "SELECT MAX(completed_at) FROM flush_runs WHERE status = 'completed'"
            ).fetchone()[0]
        return isoparse(value) if value else None

    def should_flush(self) -> bool:
        return bool(self.finalizable_event_dates())

    def maybe_flush(self) -> Optional[dict[str, Any]]:
        if not self.should_flush():
            return None
        return self.flush_pending()

    def flush_pending(self, cutoff_date: Optional[date] = None) -> Optional[dict[str, Any]]:
        if not self.flush_lock.acquire(blocking=False):
            return None
        flush_id = uuid.uuid4().hex
        now = iso_utc(utc_now())
        tmp_paths: list[Path] = []
        cutoff = (cutoff_date or utc_now().date()).isoformat()
        try:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    rows = conn.execute(
                        """
                        SELECT * FROM ingest_events
                        WHERE flush_id IS NULL AND event_date < ?
                        ORDER BY event_date, start_ts, event_id
                        """,
                        (cutoff,),
                    ).fetchall()
                    if not rows:
                        conn.rollback()
                        return None
                    conn.execute(
                        "INSERT INTO flush_runs (flush_id, status, started_at, row_count) VALUES (?, 'writing', ?, ?)",
                        (flush_id, now, len(rows)),
                    )
                    conn.execute(
                        "UPDATE ingest_events SET flush_id = ? WHERE flush_id IS NULL AND event_date < ?",
                        (flush_id, cutoff),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            grouped: dict[str, list[sqlite3.Row]] = {}
            for row in rows:
                grouped.setdefault(row["event_date"], []).append(row)

            file_records: list[dict[str, Any]] = []
            for event_date, date_rows in grouped.items():
                final_dir = self.parquet_root / "health_events" / f"event_date={event_date}"
                final_dir.mkdir(parents=True, exist_ok=True)
                final_path = final_dir / "events.parquet"
                tmp_path = self.parquet_root / ".tmp" / f"events-{flush_id}-{event_date}.parquet"
                tmp_paths.append(tmp_path)
                self.write_daily_parquet(tmp_path, date_rows, existing_path=final_path if final_path.exists() else None)
                stats = self.parquet_stats(tmp_path)
                os.replace(tmp_path, final_path)
                file_records.append({
                    "event_date": event_date,
                    "path": str(final_path.relative_to(self.parquet_root)),
                    "row_count": stats["row_count"],
                    "min_start_ts": stats["min_start_ts"],
                    "max_start_ts": stats["max_start_ts"],
                })

            completed_at = iso_utc(utc_now())
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for file_record in file_records:
                        conn.execute(
                            """
                            INSERT INTO parquet_files
                              (flush_id, event_date, path, row_count, min_start_ts, max_start_ts, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(path) DO UPDATE SET
                              flush_id = excluded.flush_id,
                              event_date = excluded.event_date,
                              row_count = excluded.row_count,
                              min_start_ts = excluded.min_start_ts,
                              max_start_ts = excluded.max_start_ts,
                              created_at = excluded.created_at
                            """,
                            (
                                flush_id,
                                file_record["event_date"],
                                file_record["path"],
                                file_record["row_count"],
                                file_record["min_start_ts"],
                                file_record["max_start_ts"],
                                completed_at,
                            ),
                        )
                    conn.execute(
                        """
                        UPDATE seen_events
                        SET flushed_at = ?, flush_id = ?
                        WHERE event_id IN (SELECT event_id FROM ingest_events WHERE flush_id = ?)
                        """,
                        (completed_at, flush_id, flush_id),
                    )
                    conn.execute("DELETE FROM ingest_events WHERE flush_id = ?", (flush_id,))
                    conn.execute(
                        "UPDATE flush_runs SET status = 'completed', completed_at = ? WHERE flush_id = ?",
                        (completed_at, flush_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            logger.info("FLUSH OK: %s rows into %s parquet files", len(rows), len(file_records))
            return {"flush_id": flush_id, "row_count": len(rows), "files": len(file_records)}
        except Exception as exc:
            error = str(exc)
            logger.exception("FLUSH FAILED")
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute("UPDATE ingest_events SET flush_id = NULL WHERE flush_id = ?", (flush_id,))
                    conn.execute(
                        "UPDATE flush_runs SET status = 'failed', completed_at = ?, error = ? WHERE flush_id = ?",
                        (iso_utc(utc_now()), error, flush_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logger.exception("Failed to mark flush failure")
            raise
        finally:
            for path in tmp_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    logger.exception("Failed to remove temp parquet file %s", path)
            self.flush_lock.release()

    def write_daily_parquet(
        self,
        path: Path,
        rows: list[sqlite3.Row],
        *,
        existing_path: Optional[Path] = None,
    ) -> None:
        values = [tuple(row[column] for column in CANONICAL_COLUMNS) for row in rows]
        path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(database=":memory:") as conn:
            conn.execute(
                """
                CREATE TABLE events (
                    event_id VARCHAR,
                    source_collection VARCHAR,
                    event_kind VARCHAR,
                    sample_uuid VARCHAR,
                    record_type VARCHAR,
                    start_ts TIMESTAMPTZ,
                    end_ts TIMESTAMPTZ,
                    event_date DATE,
                    duration_seconds DOUBLE,
                    value DOUBLE,
                    unit VARCHAR,
                    source_name VARCHAR,
                    device VARCHAR,
                    metadata_json VARCHAR,
                    payload_json VARCHAR,
                    received_at TIMESTAMPTZ
                )
                """
            )
            conn.executemany(
                f"INSERT INTO events VALUES ({', '.join(['?'] * len(CANONICAL_COLUMNS))})",
                values,
            )
            if existing_path:
                existing_literal = sql_string_literal(str(existing_path))
                output_literal = sql_string_literal(str(path))
                conn.execute(
                    f"""
                    COPY (
                        SELECT {', '.join(CANONICAL_COLUMNS)}
                        FROM (
                            SELECT
                                *,
                                row_number() OVER (
                                    PARTITION BY event_id
                                    ORDER BY received_at DESC, start_ts DESC
                                ) AS row_num
                            FROM (
                                SELECT {', '.join(CANONICAL_COLUMNS)}
                                FROM read_parquet({existing_literal})
                                UNION ALL
                                SELECT {', '.join(CANONICAL_COLUMNS)}
                                FROM events
                            )
                        )
                        WHERE row_num = 1
                        ORDER BY start_ts, event_id
                    ) TO {output_literal} (FORMAT PARQUET)
                    """
                )
            else:
                output_literal = sql_string_literal(str(path))
                conn.execute(
                    f"""
                    COPY (
                        SELECT {', '.join(CANONICAL_COLUMNS)}
                        FROM events
                        ORDER BY start_ts, event_id
                    ) TO {output_literal} (FORMAT PARQUET)
                    """
                )

    def parquet_stats(self, path: Path) -> dict[str, Any]:
        with duckdb.connect(database=":memory:") as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS row_count,
                    CAST(MIN(start_ts) AS VARCHAR) AS min_start_ts,
                    CAST(MAX(start_ts) AS VARCHAR) AS max_start_ts
                FROM read_parquet(?)
                """,
                (str(path),),
            ).fetchone()
        return {
            "row_count": int(row[0]),
            "min_start_ts": row[1],
            "max_start_ts": row[2],
        }

    def info(self) -> dict[str, Any]:
        today = utc_now().date().isoformat()
        with self.connect() as conn:
            pending = int(conn.execute("SELECT COUNT(*) FROM ingest_events WHERE flush_id IS NULL").fetchone()[0])
            finalizable = int(conn.execute(
                "SELECT COUNT(*) FROM ingest_events WHERE flush_id IS NULL AND event_date < ?",
                (today,),
            ).fetchone()[0])
            seen = int(conn.execute("SELECT COUNT(*) FROM seen_events").fetchone()[0])
            parquet_files = int(conn.execute("SELECT COUNT(*) FROM parquet_files").fetchone()[0])
            parquet_rows = int(conn.execute("SELECT COALESCE(SUM(row_count), 0) FROM parquet_files").fetchone()[0])
            latest_seen = conn.execute("SELECT MAX(last_received_at) FROM seen_events").fetchone()[0]
            latest_flush = conn.execute("SELECT MAX(completed_at) FROM flush_runs WHERE status = 'completed'").fetchone()[0]
            flush_rows = conn.execute(
                "SELECT status, COUNT(*) FROM flush_runs GROUP BY status"
            ).fetchall()
            flush_runs = {row["status"]: int(row["COUNT(*)"]) for row in flush_rows}
        return {
            "server_time": iso_utc(utc_now()),
            "last_ingest_at": latest_seen,
            "last_flush_at": latest_flush,
            "total_items": seen,
            "tables": {
                "seen_events": seen,
                "pending_events": pending,
                "parquet_rows": parquet_rows,
                "parquet_files": parquet_files,
            },
            "pending_items": pending,
            "hot_items": pending - finalizable,
            "finalizable_items": finalizable,
            "parquet_rows": parquet_rows,
            "parquet_files": parquet_files,
            "flush_runs": flush_runs,
            "data_dir": str(self.data_dir),
            "sqlite_path": str(self.sqlite_path),
            "parquet_root": str(self.parquet_root),
        }


storage = HealthStorage.from_env()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/ingest", dependencies=[Depends(verify_api_key)])
def ingest_batch(payload: BatchPayload):
    """Main ingestion endpoint — iOS app posts batches here."""
    counts = {
        "records": len(payload.records),
        "workouts": len(payload.workouts),
        "activity_summaries": len(payload.activity_summaries),
        "electrocardiograms": len(payload.electrocardiograms),
        "workout_routes": len(payload.workout_routes),
        "heartbeat_series": len(payload.heartbeat_series),
        "audiograms": len(payload.audiograms),
        "state_of_mind": len(payload.state_of_mind),
        "correlations": len(payload.correlations),
    }
    nonempty = {k: v for k, v in counts.items() if v > 0}
    total = sum(counts.values())
    logger.info("INGEST: %s items — %s", total, nonempty)
    t0 = time.time()

    try:
        events = canonicalize_payload(payload)
        accepted = storage.store_events(events)
        flush_result = storage.maybe_flush()
    except Exception:
        logger.exception("INGEST FAILED")
        raise

    elapsed = time.time() - t0
    logger.info("INGEST OK: %s items accepted as %s events in %.2fs", total, accepted, elapsed)
    response: dict[str, Any] = {"status": "ok", "inserted": counts}
    if flush_result:
        response["flush"] = flush_result
    return response


@app.post("/register", dependencies=[Depends(verify_api_key)])
def register_user(payload: RegisterPayload):
    """Register or update the user profile with HealthKit characteristics."""
    storage.store_user_profile(payload)
    logger.info("REGISTER: user '%s'", payload.name)
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/info", dependencies=[Depends(verify_api_key)])
def info():
    return storage.info()
