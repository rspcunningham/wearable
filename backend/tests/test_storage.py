import os
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

os.environ["HEALTHSYNC_DATA_DIR"] = tempfile.mkdtemp(prefix="healthsync-test-import-")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
from fastapi.testclient import TestClient

import main
from main import (
    ActivitySummary,
    AudiogramRecord,
    AudiogramSensitivityPoint,
    BatchPayload,
    CorrelationRecord,
    ElectrocardiogramRecord,
    ElectrocardiogramVoltageMeasurement,
    HealthRecord,
    HeartbeatSeriesBeat,
    HeartbeatSeriesRecord,
    HealthStorage,
    RegisterPayload,
    StateOfMindRecord,
    Workout,
    WorkoutRouteLocation,
    WorkoutRouteRecord,
    canonicalize_payload,
)


def make_storage(tmp_path: Path) -> HealthStorage:
    return HealthStorage(
        data_dir=tmp_path,
        sqlite_path=tmp_path / "ingest.sqlite",
        parquet_root=tmp_path / "parquet",
    )


def test_canonicalize_all_payload_collections() -> None:
    start = "2026-04-24T10:00:00Z"
    end = "2026-04-24T10:01:00Z"
    payload = BatchPayload(
        records=[
            HealthRecord(
                sample_uuid="record-1",
                record_type="HKQuantityTypeIdentifierHeartRate",
                value=72,
                unit="count/min",
                start_date=start,
                end_date=end,
                device="Watch",
                source_name="Health",
                metadata={"source": "test"},
            )
        ],
        workouts=[
            Workout(
                sample_uuid="workout-1",
                workout_type="running",
                start_date=start,
                end_date=end,
                duration_seconds=60,
                source_name="Fitness",
            )
        ],
        activity_summaries=[ActivitySummary(date="2026-04-24")],
        electrocardiograms=[
            ElectrocardiogramRecord(
                sample_uuid="ecg-1",
                start_date=start,
                end_date=end,
                number_of_voltage_measurements=2,
                classification_code=1,
                symptoms_status_code=1,
                voltage_measurements=[
                    ElectrocardiogramVoltageMeasurement(
                        time_since_sample_start=0.5,
                        lead_values={"apple_watch_similar_to_lead_i": 0.001},
                    )
                ],
            )
        ],
        workout_routes=[
            WorkoutRouteRecord(
                sample_uuid="route-1",
                start_date=start,
                end_date=end,
                locations=[
                    WorkoutRouteLocation(
                        timestamp=start,
                        latitude=1,
                        longitude=2,
                        altitude=3,
                        horizontal_accuracy=4,
                        vertical_accuracy=5,
                        course=6,
                        speed=7,
                    )
                ],
            )
        ],
        heartbeat_series=[
            HeartbeatSeriesRecord(
                sample_uuid="heartbeats-1",
                start_date=start,
                end_date=end,
                beats=[
                    HeartbeatSeriesBeat(time_since_series_start=0.7, preceded_by_gap=False),
                    HeartbeatSeriesBeat(time_since_series_start=1.5, preceded_by_gap=True),
                ],
            )
        ],
        audiograms=[
            AudiogramRecord(
                sample_uuid="audiogram-1",
                start_date=start,
                end_date=end,
                sensitivity_points=[
                    AudiogramSensitivityPoint(
                        frequency_hz=1000,
                        left_ear_sensitivity_dbhl=10,
                        right_ear_sensitivity_dbhl=12,
                    )
                ],
            )
        ],
        state_of_mind=[
            StateOfMindRecord(
                sample_uuid="mood-1",
                start_date=start,
                end_date=end,
                kind_code=1,
                valence=0.5,
                valence_classification_code=5,
                label_codes=[19],
                association_codes=[8],
            )
        ],
        correlations=[
            CorrelationRecord(
                sample_uuid="correlation-1",
                correlation_type="blood_pressure",
                start_date=start,
                end_date=end,
            )
        ],
    )

    events = canonicalize_payload(
        payload,
        received_at=datetime(2026, 4, 24, 11, 0, tzinfo=timezone.utc),
    )

    assert {event.event_kind for event in events} == {
        "health_record",
        "workout",
        "activity_summary",
        "electrocardiogram",
        "workout_route",
        "heartbeat_series",
        "audiogram",
        "state_of_mind",
        "correlation",
    }
    heartbeat = next(event for event in events if event.event_kind == "heartbeat_series")
    assert heartbeat.event_id == "heartbeat_series:heartbeats-1"
    assert heartbeat.start_ts == "2026-04-24T10:00:00+00:00"
    assert heartbeat.end_ts == "2026-04-24T10:01:00+00:00"
    assert heartbeat.duration_seconds == 60
    assert '"time_since_series_start":0.7' in heartbeat.payload_json


def test_store_flush_and_skip_duplicate_after_flush(tmp_path: Path) -> None:
    storage = make_storage(tmp_path)
    payload = BatchPayload(
        records=[
            HealthRecord(
                sample_uuid="record-1",
                record_type="steps",
                value=10,
                unit="count",
                start_date="2026-04-23T10:00:00Z",
            ),
            HealthRecord(
                sample_uuid="record-2",
                record_type="steps",
                value=20,
                unit="count",
                start_date="2026-04-23T10:01:00Z",
            ),
        ]
    )
    events = canonicalize_payload(payload)

    assert storage.store_events(events) == 2
    assert storage.pending_count() == 2
    result = storage.flush_pending(cutoff_date=date(2026, 4, 24))
    assert result is not None
    assert result["row_count"] == 2
    assert storage.pending_count() == 0

    parquet_files = list((tmp_path / "parquet" / "health_events").glob("event_date=*/events.parquet"))
    assert len(parquet_files) == 1
    with duckdb.connect(database=":memory:") as conn:
        rows = conn.execute("SELECT event_kind, COUNT(*) FROM read_parquet(?) GROUP BY event_kind", (str(parquet_files[0]),)).fetchall()
    assert rows == [("health_record", 2)]

    assert storage.store_events(events) == 2
    assert storage.pending_count() == 0

    with sqlite3.connect(tmp_path / "ingest.sqlite") as conn:
        seen_count = conn.execute("SELECT COUNT(*) FROM seen_events WHERE flushed_at IS NOT NULL").fetchone()[0]
    assert seen_count == 2


def test_maybe_flush_only_finalizes_prior_utc_days(tmp_path: Path, monkeypatch) -> None:
    storage = make_storage(tmp_path)
    monkeypatch.setattr(main, "utc_now", lambda: datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc))
    events = canonicalize_payload(
        BatchPayload(
            records=[
                HealthRecord(
                    sample_uuid="record-1",
                    record_type="steps",
                    start_date="2026-04-24T10:00:00Z",
                ),
                HealthRecord(
                    sample_uuid="record-2",
                    record_type="steps",
                    start_date="2026-04-23T10:00:00Z",
                )
            ]
        )
    )
    storage.store_events(events)

    assert storage.maybe_flush() is not None
    assert storage.pending_count() == 1
    assert storage.finalizable_event_dates() == []


def test_late_arriving_finalized_day_rewrites_single_daily_file(tmp_path: Path) -> None:
    storage = make_storage(tmp_path)
    first_events = canonicalize_payload(
        BatchPayload(
            records=[
                HealthRecord(
                    sample_uuid="record-1",
                    record_type="steps",
                    start_date="2026-04-23T10:00:00Z",
                )
            ]
        )
    )
    storage.store_events(first_events)
    storage.flush_pending(cutoff_date=date(2026, 4, 24))

    late_events = canonicalize_payload(
        BatchPayload(
            records=[
                HealthRecord(
                    sample_uuid="record-2",
                    record_type="steps",
                    start_date="2026-04-23T11:00:00Z",
                )
            ]
        )
    )
    storage.store_events(late_events)
    storage.flush_pending(cutoff_date=date(2026, 4, 24))

    parquet_files = list((tmp_path / "parquet" / "health_events").glob("event_date=*/events.parquet"))
    assert len(parquet_files) == 1
    with duckdb.connect(database=":memory:") as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM read_parquet(?)", (str(parquet_files[0]),)).fetchone()[0]
    assert row_count == 2
    with sqlite3.connect(tmp_path / "ingest.sqlite") as conn:
        manifest_count = conn.execute("SELECT COUNT(*) FROM parquet_files").fetchone()[0]
        manifest_rows = conn.execute("SELECT row_count FROM parquet_files").fetchone()[0]
    assert manifest_count == 1
    assert manifest_rows == 2


def test_recover_unfinished_flush_marks_run_failed_and_releases_rows(tmp_path: Path) -> None:
    storage = make_storage(tmp_path)
    events = canonicalize_payload(
        BatchPayload(
            records=[
                HealthRecord(
                    sample_uuid="record-1",
                    record_type="steps",
                    start_date="2026-04-24T10:00:00Z",
                )
            ]
        )
    )
    storage.store_events(events)

    with sqlite3.connect(tmp_path / "ingest.sqlite") as conn:
        conn.execute(
            "INSERT INTO flush_runs (flush_id, status, started_at, row_count) VALUES ('stuck', 'writing', '2026-04-24T00:00:00+00:00', 1)"
        )
        conn.execute("UPDATE ingest_events SET flush_id = 'stuck'")
        conn.commit()

    recovered = make_storage(tmp_path)

    assert recovered.pending_count() == 1
    with sqlite3.connect(tmp_path / "ingest.sqlite") as conn:
        status = conn.execute("SELECT status FROM flush_runs WHERE flush_id = 'stuck'").fetchone()[0]
    assert status == "failed"


def test_register_profile_is_stored_as_json(tmp_path: Path) -> None:
    storage = make_storage(tmp_path)
    storage.store_user_profile(
        RegisterPayload(
            name="Robin",
            biological_sex_code=2,
            blood_type_code=1,
        )
    )

    with sqlite3.connect(tmp_path / "ingest.sqlite") as conn:
        payload_json = conn.execute("SELECT payload_json FROM user_profiles WHERE id = 1").fetchone()[0]
    assert '"name":"Robin"' in payload_json
    assert '"biological_sex":"male"' in payload_json


def test_ingest_and_info_routes_keep_public_contract(tmp_path: Path, monkeypatch) -> None:
    route_storage = make_storage(tmp_path)
    monkeypatch.setattr(main, "utc_now", lambda: datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(main, "storage", route_storage)
    client = TestClient(main.app)

    response = client.post(
        "/ingest",
        headers={"X-API-Key": "change-me-before-deploying"},
        json={
            "records": [
                {
                    "sample_uuid": "record-route-1",
                    "record_type": "steps",
                    "value": 10,
                    "unit": "count",
                    "start_date": "2026-04-23T10:00:00Z",
                }
            ]
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["inserted"]["records"] == 1
    assert body["flush"]["row_count"] == 1

    info_response = client.get(
        "/info",
        headers={"X-API-Key": "change-me-before-deploying"},
    )
    assert info_response.status_code == 200
    info = info_response.json()
    assert "server_time" in info
    assert info["total_items"] == 1
    assert info["tables"]["seen_events"] == 1
    assert info["tables"]["parquet_rows"] == 1
