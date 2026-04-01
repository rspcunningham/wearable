# iOS Initial Testing Plan

## Goal

Take the current repo from a partial prototype to a state where it can be installed on a physical iPhone and used for the first end-to-end HealthKit sync test against a live server.

This plan assumes:

- The Swift files in this repo will be added to a new Xcode iOS app project.
- Initial testing will use direct install from Xcode to a personal iPhone.
- The Python server will be reachable over HTTPS from the phone.

## Current State

### What exists

- A FastAPI server in [main.py](/Users/robin/Desktop/aperture/main.py)
- SwiftUI app entry point in [HealthSyncApp.swift](/Users/robin/Desktop/aperture/HealthSyncApp.swift)
- HealthKit sync manager in [HealthKitManager.swift](/Users/robin/Desktop/aperture/HealthKitManager.swift)
- Simple HTTP client in [ServerClient.swift](/Users/robin/Desktop/aperture/ServerClient.swift)
- Setup notes in [README.md](/Users/robin/Desktop/aperture/README.md)

### Current blockers

- The iOS code does not compile as-is due to the ECG type check in [HealthKitManager.swift](/Users/robin/Desktop/aperture/HealthKitManager.swift).
- The app hardcodes placeholder server configuration in [ServerClient.swift](/Users/robin/Desktop/aperture/ServerClient.swift).
- Initial sync completion is recorded even if uploads fail.
- The observer path couples HealthKit callbacks to network retries, which is risky for background delivery.
- Activity summaries are fetched in a way that will duplicate data on the server.
- Anchored queries ignore deletions and do not fully protect against duplicate or missed records.
- The repo is not an Xcode project yet, so capabilities, entitlements, and plist settings are not configured.

## Definition Of Done

The app is ready for initial iPhone testing when all of the following are true:

- The Swift app builds cleanly for a physical iPhone target.
- HealthKit permissions are granted on-device for the intended sample types.
- The app can perform a first-run historical sync without marking completion on failure.
- The phone can reach the server over HTTPS and authenticate successfully.
- Server requests are observable in logs and inserted into SQLite as expected.
- A manual sync and at least one background-triggered sync succeed during testing.

## Work Plan

### Phase 1: Create The Xcode Project

Create a new iOS SwiftUI app project and add:

- [HealthSyncApp.swift](/Users/robin/Desktop/aperture/HealthSyncApp.swift)
- [HealthKitManager.swift](/Users/robin/Desktop/aperture/HealthKitManager.swift)
- [ServerClient.swift](/Users/robin/Desktop/aperture/ServerClient.swift)

Configure the project with:

- iOS deployment target matching the intended device OS
- A valid bundle identifier
- Your Apple developer team
- Automatic signing for initial device testing

Project configuration to add:

- `HealthKit` capability
- Background delivery support for HealthKit
- `NSHealthShareUsageDescription`
- `NSHealthUpdateUsageDescription`

Acceptance criteria:

- Xcode project opens cleanly
- Files are compiled into the app target
- Signing is valid for a physical device build

### Phase 2: Fix Compile-Time Issues

Update the Swift code so it builds on the selected iOS SDK:

- Fix the ECG object type handling in [HealthKitManager.swift](/Users/robin/Desktop/aperture/HealthKitManager.swift)
- Remove or replace deprecated HealthKit identifiers that should not be carried into device testing
- Clean up Swift concurrency warnings where practical, especially around async callbacks

Acceptance criteria:

- The app builds without errors for a physical iPhone
- Warnings are reduced to known and acceptable items only

### Phase 3: Make Runtime Configuration Safe

Replace placeholder configuration in [ServerClient.swift](/Users/robin/Desktop/aperture/ServerClient.swift) with a proper app configuration source.

Recommended approach:

- Add `SERVER_BASE_URL` and `HEALTH_API_KEY` as build settings or `Info.plist` values
- Read them at runtime
- Fail fast with a visible error state if they are missing or malformed

Do not keep production values hardcoded in source for ongoing use.

Acceptance criteria:

- The app reads a valid base URL and API key at runtime
- Misconfiguration is obvious immediately

### Phase 4: Correct Sync Semantics

The current sync flow is not safe enough for device testing. Update it so the app behaves predictably:

- Change the upload layer to return success or failure instead of only printing logs
- Mark initial seeding complete only after the required uploads succeed
- Separate health data fetch from upload outcome tracking
- Ensure anchors are advanced correctly even when there are zero new samples
- Decide how deleted HealthKit objects are handled and implement that path

Recommended behavior:

- HealthKit fetch completes quickly
- Upload result is recorded explicitly
- Failed uploads remain retryable
- No one-time state is committed before data is safely persisted remotely

Acceptance criteria:

- First launch does not permanently skip historical sync after a failed network call
- Repeated launches resume correctly after interruption
- No obvious duplicate explosion occurs during retries

### Phase 5: Improve Background Delivery Behavior

The current observer path performs network work inside the observer lifecycle. For initial device testing, restructure it so it is less likely to miss background deadlines.

Required changes:

- Do not hold HealthKit observer completion open while retrying HTTP calls
- Queue or stage fetched data before upload
- Make background-triggered sync lightweight
- Keep manual sync available for explicit recovery

Recommended implementation options:

- In-memory queue for the first test build if scope must stay small
- Preferably a persisted local queue if you want resilience across app termination

Acceptance criteria:

- Observer callbacks return quickly
- A temporary network failure does not silently lose data
- Manual sync can flush pending work

### Phase 6: Reduce First-Run Load

The current manager registers many types and fetches with `HKObjectQueryNoLimit`. That is acceptable as a prototype but too heavy for a predictable first-run test.

Recommended changes:

- Consider narrowing the initial type set for the first device test
- Batch uploads across types instead of POSTing repeatedly per type
- Page very large historical syncs instead of pulling everything into memory at once
- Limit the first test to a known set such as steps, heart rate, sleep, workouts, and activity summaries

Suggested first-test type set:

- `stepCount`
- `heartRate`
- `sleepAnalysis`
- `activeEnergyBurned`
- `distanceWalkingRunning`
- workouts
- activity summaries

This reduces moving parts while still proving the architecture.

Acceptance criteria:

- Initial sync completes in a reasonable time on a real phone
- Network traffic is manageable
- The app stays responsive during the first run

### Phase 7: Fix Activity Summary Handling

Activity summaries currently fetch a rolling two-year range and the backend blindly inserts results. That will create duplicates.

Choose one of these strategies:

- Add a server-side upsert key on summary date and always overwrite
- Track the last synced summary date and only request the needed range

For initial testing, server-side upsert by date is the simpler option.

Acceptance criteria:

- Re-running the app does not accumulate duplicate summaries for the same day

### Phase 8: Align The Server For Safe Client Testing

Some iOS-side fixes require matching backend support.

Backend changes to make before device testing:

- Add stable deduplication keys where needed
- Add upsert behavior for activity summaries
- Decide whether health records need a unique client-generated identifier
- Decide how delete events should be represented if delete handling is implemented
- Confirm API key auth is enabled with a non-default key
- Confirm server is exposed through HTTPS with a valid certificate

Operational tasks:

- Install Python dependencies from [requirements.txt](/Users/robin/Desktop/aperture/requirements.txt)
- Start the FastAPI app
- Verify `/ping`
- Verify authenticated `/health/summary`

Acceptance criteria:

- The phone can reach the server over HTTPS
- Authenticated requests succeed
- Inserted records are visible in SQLite and query endpoints

### Phase 9: Add Minimal Observability

Before first device install, add enough observability to debug failures quickly.

Recommended minimum:

- Structured app logs for authorization, fetch start, fetch count, upload success, upload failure, anchor save, and seed completion
- Clear server logs for request size, inserted counts, and auth failures
- A visible app status string for the current sync state

Useful debug states:

- authorization denied
- configuration invalid
- initial sync in progress
- initial sync failed
- background sync pending
- last upload succeeded

Acceptance criteria:

- A failed test run produces enough information to identify the failing stage without attaching a debugger

### Phase 10: Prepare The First Device Build

Before installing on the phone:

- Set a test API key
- Set the test server base URL
- Build with debug logging enabled
- Confirm the app target includes HealthKit entitlement
- Confirm the selected device is signed and trusted

On the iPhone:

- Enable Developer Mode if required
- Trust the developer certificate if prompted
- Ensure the device has Health data available to read
- Keep the app in the foreground for the first historical sync

Acceptance criteria:

- The app installs successfully on the phone
- The app launches and prompts for Health permissions

## Test Sequence

### Test 1: Configuration Sanity Check

- Launch the app
- Confirm it does not crash on startup
- Confirm misconfiguration messaging is clear if server config is missing

Expected result:

- App reaches idle or setup state cleanly

### Test 2: Authorization Flow

- Grant Health permissions for the selected sample types
- Confirm the app records the permission outcome

Expected result:

- Authorization succeeds and setup continues

### Test 3: First Historical Sync

- Trigger the initial full sync
- Keep the app active until the run completes
- Watch app logs and server logs simultaneously

Expected result:

- The server receives batched payloads
- SQLite contains records and workouts
- Seed completion is recorded only after successful upload

### Test 4: Query Validation

Use the server API to verify:

- `/health/records`
- `/health/workouts`
- `/health/summary`

Expected result:

- Counts and timestamps reflect the data sent by the phone

### Test 5: Retry Behavior

- Simulate a temporary network failure
- Trigger another sync
- Restore connectivity

Expected result:

- Failed uploads are retried or remain pending
- The app does not falsely report full success

### Test 6: Reopen Behavior

- Kill the app after a partial or successful sync
- Relaunch it

Expected result:

- The app resumes from saved state correctly
- Historical sync is not repeated unnecessarily

### Test 7: Background Trigger Check

- Generate or wait for a new HealthKit sample on-device
- Background the app
- Reopen later and inspect logs and server state

Expected result:

- New data eventually reaches the server
- Existing data is not duplicated excessively

## Recommended Build Order

Implement in this order:

1. Create Xcode project and capabilities
2. Fix compile errors and SDK incompatibilities
3. Move config out of hardcoded placeholders
4. Return upload success or failure from the network layer
5. Fix seed completion logic
6. Restructure observer handling so completion is not blocked on uploads
7. Add batching and reduce the initial type set
8. Fix activity summary duplication
9. Align backend dedupe and upsert behavior
10. Add minimal observability
11. Install on iPhone and run the first end-to-end test

## Scope Recommendation For Initial Testing

Do not try to validate every HealthKit type on the first device pass.

Recommended first milestone:

- Quantity samples for a small trusted set
- Sleep category samples
- Workouts
- Activity summaries
- Manual sync
- One background sync proof

Defer until after the first successful device test:

- Full type coverage
- ECG and entitlement-sensitive types
- Advanced delete reconciliation
- Long-lived offline queue polish
- Production-grade analytics and dashboards

## Risks To Watch Closely

- HealthKit background delivery timing is not immediate in practice
- First-run historical sync may be large and slow
- Server deduplication gaps can make test results misleading
- Local app state can say "seeded" even when server state is incomplete unless fixed
- Hardcoded or missing config can waste the first device test cycle

## Ready-To-Test Checklist

- Xcode project created
- HealthKit capability enabled
- Usage description keys added
- Physical device signing working
- Swift build passes
- Base URL and API key injected from config
- HTTPS server reachable from phone
- API key validated on server
- Upload success and failure surfaced in app state
- Seeding only completes on success
- Background observer path no longer waits on network retries
- Activity summaries no longer duplicate
- Minimal server and app logs in place
- Test dataset available on the iPhone

## Suggested Next Deliverable

After this planning stage, the next practical implementation milestone is:

"Make the app compile, externalize configuration, and make initial sync success/failure explicit."

That milestone is the shortest path to a meaningful first device install.
