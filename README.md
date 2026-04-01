# Health Sync

Apple Health to personal server sync, with:
- an iOS app in [`ios/`](./ios)
- a FastAPI + SQLite backend in [`backend/`](./backend)
- supporting docs in [`docs/`](./docs)

The goal is broad Apple Health coverage for personal archival and analysis, including quantity samples, category samples, workouts, activity summaries, ECGs, workout routes, heartbeat series, audiograms, state of mind, correlations, and a profile snapshot.

## Repo Layout

```text
.
├── backend/
│   ├── main.py
│   ├── pyproject.toml
│   └── uv.lock
├── docs/
│   ├── IOS_INITIAL_TESTING_PLAN.md
│   └── ios_cd.md
├── ios/
│   ├── project.yml
│   ├── AppConfig.swift
│   ├── HealthSyncApp.swift
│   ├── HealthKitManager.swift
│   ├── PendingUploadQueue.swift
│   ├── ServerClient.swift
│   ├── Config/
│   └── HealthSync/
└── README.md
```

## Current Status

The current app/backend pair:
- builds from the CLI
- supports iOS 18.0+
- uses `xcodegen` as the source of truth for the Xcode project
- reads runtime server config from build settings and `Info.plist`
- posts HealthKit batches to the backend over HTTPS

The generated Xcode project lives at [`ios/HealthSync.xcodeproj`](./ios/HealthSync.xcodeproj), but [`ios/project.yml`](./ios/project.yml) is the file that should be edited.

## iOS App

Core files:
- [`ios/HealthSyncApp.swift`](./ios/HealthSyncApp.swift): app entry point and startup flow
- [`ios/HealthKitManager.swift`](./ios/HealthKitManager.swift): authorization, observers, anchored sync, serialization
- [`ios/ServerClient.swift`](./ios/ServerClient.swift): HTTP client for `/ingest`
- [`ios/AppConfig.swift`](./ios/AppConfig.swift): runtime config loading
- [`ios/PendingUploadQueue.swift`](./ios/PendingUploadQueue.swift): local pending-upload queue

Project/config files:
- [`ios/project.yml`](./ios/project.yml): XcodeGen spec
- [`ios/Config/Debug.xcconfig`](./ios/Config/Debug.xcconfig)
- [`ios/Config/Release.xcconfig`](./ios/Config/Release.xcconfig)
- [`ios/HealthSync/Info.plist`](./ios/HealthSync/Info.plist)
- [`ios/HealthSync/HealthSync.entitlements`](./ios/HealthSync/HealthSync.entitlements)

### iOS Prerequisites

- Xcode installed
- Command Line Tools installed
- `xcodegen` installed
- an Apple ID signed into Xcode on this machine at least once
- a physical iPhone for real HealthKit testing

Check tools:

```bash
xcodebuild -version
xcodegen --version
```

### iOS Configuration

Edit [`ios/Config/Debug.xcconfig`](./ios/Config/Debug.xcconfig) for local testing.

Required values:
- `PRODUCT_BUNDLE_IDENTIFIER`
- `SERVER_BASE_URL`
- `HEALTH_API_KEY`

Recommended for device signing:
- `DEVELOPMENT_TEAM`

Example:

```xcconfig
PRODUCT_BUNDLE_IDENTIFIER = com.yourname.healthsync.dev
DEVELOPMENT_TEAM = YOURTEAMID
SERVER_BASE_URL = https://health.example.com
HEALTH_API_KEY = replace-me
```

`Info.plist` injects those values into the app, and `AppConfig.swift` validates them at runtime.

### Generate The Xcode Project

Run from the repo root:

```bash
cd /Users/robin/Desktop/aperture
xcodegen generate --spec ios/project.yml
```

Or from inside `ios/`:

```bash
cd /Users/robin/Desktop/aperture/ios
xcodegen generate --spec project.yml
```

### Build The iOS App

Typecheck:

```bash
swiftc -typecheck \
  -target arm64-apple-ios18.0-simulator \
  -sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)" \
  ios/AppConfig.swift \
  ios/HealthSyncApp.swift \
  ios/HealthKitManager.swift \
  ios/ServerClient.swift \
  ios/PendingUploadQueue.swift
```

Simulator build:

```bash
xcodebuild \
  -project ios/HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -sdk iphonesimulator \
  build \
  CODE_SIGNING_ALLOWED=NO
```

Device destination discovery:

```bash
xcodebuild -project ios/HealthSync.xcodeproj -scheme HealthSync -showdestinations
xcrun xctrace list devices
```

Device build:

```bash
xcodebuild \
  -project ios/HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'platform=iOS,name=Your iPhone' \
  build
```

For a fuller CLI deployment workflow, see [`docs/ios_cd.md`](./docs/ios_cd.md).

## Backend

Core files:
- [`backend/main.py`](./backend/main.py): FastAPI app, SQLite schema, ingest route
- [`backend/pyproject.toml`](./backend/pyproject.toml): project metadata and dependencies

### Backend Setup

```bash
cd /Users/robin/Desktop/aperture/backend
uv sync
export HEALTH_API_KEY="replace-me"
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

For real device use, put the backend behind HTTPS. iOS HealthKit sync should not rely on plain HTTP.

### Backend Verification

Syntax check:

```bash
python3 -m py_compile backend/main.py
```

Health check:

```bash
curl http://127.0.0.1:8000/ping
```

Authenticated summary:

```bash
curl \
  -H "X-API-Key: $HEALTH_API_KEY" \
  http://127.0.0.1:8000/health/summary
```

## API Surface

- `POST /ingest` — main batch ingestion endpoint (requires `X-API-Key` header)
- `GET /health` — health check

## HealthKit Coverage

Implemented today:
- broad non-clinical quantity and category coverage
- workouts
- activity summaries
- ECGs
- workout routes
- heartbeat series
- audiograms
- state of mind
- correlations
- profile snapshot data:
  - date of birth
  - biological sex
  - blood type
  - Fitzpatrick skin type
  - wheelchair use
  - activity move mode

Not currently implemented:
- scored assessments such as GAD-7 and PHQ-9
- vision prescriptions
- clinical/EHR/FHIR records

## Docs

- [`docs/IOS_INITIAL_TESTING_PLAN.md`](./docs/IOS_INITIAL_TESTING_PLAN.md): higher-level roadmap
- [`docs/ios_cd.md`](./docs/ios_cd.md): CLI project generation, build, signing, and install workflow

## Notes

- Apple Watch data reaches this app through HealthKit on the iPhone.
- Background delivery is near-real-time, not instant.
- The app must be opened at least once after install before background delivery can begin.
- A physical device is required to meaningfully test HealthKit authorization and background behavior.
