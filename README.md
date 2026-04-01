# Health Sync

Real-time Apple Health → Python server pipeline.

---

## Server Setup

### 1. Deploy to your VPS

```bash
git clone <your-repo>
cd health-server

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set your API key
export HEALTH_API_KEY="generate-a-long-random-string-here"

# Run
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 2. Put it behind nginx + SSL (required — iOS won't POST to plain HTTP)

```nginx
server {
    server_name health.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }
}
```

Then: `certbot --nginx -d health.yourdomain.com`

### 3. Run as a service (systemd)

```ini
# /etc/systemd/system/health-sync.service
[Unit]
Description=Health Sync Server

[Service]
WorkingDirectory=/path/to/health-server
ExecStart=/path/to/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
Environment=HEALTH_API_KEY=your-key-here
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable health-sync
systemctl start health-sync
```

---

## iOS App Setup

### 1. Create Xcode project

- New project → App → SwiftUI
- Bundle ID: `com.yourname.healthsync`
- Minimum deployment: iOS 16+

### 2. Add files

Copy these into your Xcode project:
- `HealthSyncApp.swift`
- `HealthKitManager.swift`
- `ServerClient.swift`

### 3. Configure ServerClient.swift

```swift
private let baseURL = "https://health.yourdomain.com"
private let apiKey  = "your-api-key-here"
```

### 4. Enable HealthKit capability

In Xcode: Target → Signing & Capabilities → + Capability → HealthKit
Check "Background Delivery"

### 5. Info.plist keys required

```xml
<key>NSHealthShareUsageDescription</key>
<string>This app syncs your health data to your personal server.</string>
<key>NSHealthUpdateUsageDescription</key>
<string>This app syncs your health data to your personal server.</string>
```

### 6. Build & install via TestFlight or direct device

For personal use, TestFlight is easiest — no App Store review needed.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/health/batch` | Ingest records (used by iOS app) |
| GET | `/health/records?record_type=X&since=2024-01-01` | Query records |
| GET | `/health/workouts?since=2024-01-01` | Query workouts |
| GET | `/health/summary` | Overview of all stored data types |
| GET | `/ping` | Health check |

All endpoints except `/ping` require `X-API-Key` header.

Auto-generated API docs: `https://health.yourdomain.com/docs`

---

## How it works

1. **First launch**: app requests HealthKit permissions, then bulk-syncs all historical data
2. **Ongoing**: HealthKit wakes the app in the background whenever new data arrives (effectively real-time for most types)
3. **Anchored queries**: only new records since last sync are fetched and posted — no duplicates
4. **Retry logic**: failed POSTs retry 3x with exponential backoff

---

## Notes

- Apple Watch data flows: Watch → iPhone HealthKit → your app → server
- "Immediate" background delivery means within a few minutes, not milliseconds
- The app must have been opened at least once after install for background delivery to work
- Some data types (ECG, clinical records) require additional Apple entitlements
