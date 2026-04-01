# iOS CLI Deployment Workflow

## Purpose

This document describes the filesystem and CLI workflow for generating, building, signing, and deploying the `HealthSync` iOS app to a physical iPhone without using the Xcode GUI.

It is intentionally separate from [IOS_INITIAL_TESTING_PLAN.md](/Users/robin/Desktop/aperture/IOS_INITIAL_TESTING_PLAN.md), which remains the higher-level implementation and testing plan.

## Project Layout

The iOS app is now defined by these files:

- [project.yml](/Users/robin/Desktop/aperture/project.yml)
- [Config/Debug.xcconfig](/Users/robin/Desktop/aperture/Config/Debug.xcconfig)
- [Config/Release.xcconfig](/Users/robin/Desktop/aperture/Config/Release.xcconfig)
- [HealthSync/Info.plist](/Users/robin/Desktop/aperture/HealthSync/Info.plist)
- [HealthSync/HealthSync.entitlements](/Users/robin/Desktop/aperture/HealthSync/HealthSync.entitlements)
- [AppConfig.swift](/Users/robin/Desktop/aperture/AppConfig.swift)
- [HealthSyncApp.swift](/Users/robin/Desktop/aperture/HealthSyncApp.swift)
- [HealthKitManager.swift](/Users/robin/Desktop/aperture/HealthKitManager.swift)
- [ServerClient.swift](/Users/robin/Desktop/aperture/ServerClient.swift)

The generated Xcode project is:

- [HealthSync.xcodeproj](/Users/robin/Desktop/aperture/HealthSync.xcodeproj)

## Source Of Truth

Do not manually edit the generated `.xcodeproj` unless absolutely necessary.

The intended workflow is:

1. Edit [project.yml](/Users/robin/Desktop/aperture/project.yml) or the config files.
2. Regenerate the project with `xcodegen generate`.
3. Build and deploy using `xcodebuild`.

## Prerequisites

Required tools:

- Xcode installed
- Command Line Tools available
- `xcodegen` installed

Check tool availability:

```bash
xcodebuild -version
xcodegen --version
```

You also need:

- An Apple ID signed into Xcode on this machine at least once
- A physical iPhone connected by USB or available over local device pairing
- Developer Mode enabled on the iPhone if required

## Configuration Files

### Build Settings

Edit [Config/Debug.xcconfig](/Users/robin/Desktop/aperture/Config/Debug.xcconfig) for device testing.

Current required values:

- `PRODUCT_BUNDLE_IDENTIFIER`
- `SERVER_BASE_URL`
- `HEALTH_API_KEY`

Recommended additional value for real device signing:

- `DEVELOPMENT_TEAM`

Suggested debug config shape:

```xcconfig
PRODUCT_BUNDLE_IDENTIFIER = com.yourname.healthsync.dev
DEVELOPMENT_TEAM = YOURTEAMID
SERVER_BASE_URL = https://your-server.example.com
HEALTH_API_KEY = your-test-api-key
```

If `DEVELOPMENT_TEAM` is not set, device signing usually fails or requires fallback behavior from local Xcode account state.

### Runtime Configuration

[HealthSync/Info.plist](/Users/robin/Desktop/aperture/HealthSync/Info.plist) injects:

- `SERVER_BASE_URL`
- `HEALTH_API_KEY`
- HealthKit usage descriptions

[AppConfig.swift](/Users/robin/Desktop/aperture/AppConfig.swift) reads those values at runtime and fails early if they are missing or malformed.

### Entitlements

[HealthSync/HealthSync.entitlements](/Users/robin/Desktop/aperture/HealthSync/HealthSync.entitlements) currently enables:

- `com.apple.developer.healthkit`
- `com.apple.developer.healthkit.background-delivery`

## Regenerating The Project

Whenever [project.yml](/Users/robin/Desktop/aperture/project.yml) changes, regenerate the Xcode project:

```bash
cd /Users/robin/Desktop/aperture
xcodegen generate
```

This rewrites [HealthSync.xcodeproj](/Users/robin/Desktop/aperture/HealthSync.xcodeproj) from the spec.

You do not need to regenerate the project when only `.swift`, `.plist`, or `.xcconfig` values change unless the project structure itself changed.

## Useful Discovery Commands

List schemes:

```bash
xcodebuild -list -project HealthSync.xcodeproj
```

List available destinations:

```bash
xcodebuild -project HealthSync.xcodeproj -scheme HealthSync -showdestinations
```

List connected devices:

```bash
xcrun xctrace list devices
```

## Simulator Build

Use this to validate the project without code signing:

```bash
cd /Users/robin/Desktop/aperture
xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -sdk iphonesimulator \
  build \
  CODE_SIGNING_ALLOWED=NO
```

This is useful for fast verification after source changes.

## Physical iPhone Build

First determine the exact device name or destination identifier:

```bash
xcodebuild -project HealthSync.xcodeproj -scheme HealthSync -showdestinations
```

Then build for the connected phone. Example using a device name:

```bash
cd /Users/robin/Desktop/aperture
xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'platform=iOS,name=Your iPhone' \
  build
```

If multiple devices have the same name, use the device identifier instead:

```bash
xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'id=YOUR-DEVICE-ID' \
  build
```

## Archive-Style Build For Device Output

If you want a clean device build artifact, use:

```bash
cd /Users/robin/Desktop/aperture
xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'generic/platform=iOS' \
  -derivedDataPath build/DerivedData \
  build
```

The built app is typically found under:

- `build/DerivedData/Build/Products/Debug-iphoneos/HealthSync.app`

## Install To Device From CLI

There are two common CLI options.

### Option 1: Use `devicectl`

Modern Xcode includes `devicectl`.

List devices:

```bash
xcrun devicectl list devices
```

Install the app:

```bash
xcrun devicectl device install app \
  --device YOUR-DEVICE-ID \
  build/DerivedData/Build/Products/Debug-iphoneos/HealthSync.app
```

Launch the app:

```bash
xcrun devicectl device process launch \
  --device YOUR-DEVICE-ID \
  com.yourname.healthsync.dev
```

Use the real bundle identifier from [Config/Debug.xcconfig](/Users/robin/Desktop/aperture/Config/Debug.xcconfig).

### Option 2: Build Directly To The Device Destination

If `xcodebuild` successfully targets the physical iPhone, Xcode often handles the install during the build step.

That is the simplest path:

```bash
xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'platform=iOS,name=Your iPhone' \
  build
```

## Recommended First Device Workflow

Use this sequence for the first real install:

1. Edit [Config/Debug.xcconfig](/Users/robin/Desktop/aperture/Config/Debug.xcconfig) with real values.
2. Set `DEVELOPMENT_TEAM`.
3. Confirm your server is reachable over HTTPS.
4. Regenerate the project only if [project.yml](/Users/robin/Desktop/aperture/project.yml) changed.
5. Run a simulator build with signing disabled.
6. List device destinations.
7. Run a physical device build.
8. If needed, install with `devicectl`.
9. Launch the app and grant Health permissions.

## Example End-To-End Command Sequence

```bash
cd /Users/robin/Desktop/aperture

xcodegen generate

xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -sdk iphonesimulator \
  build \
  CODE_SIGNING_ALLOWED=NO

xcodebuild -project HealthSync.xcodeproj -scheme HealthSync -showdestinations

xcodebuild \
  -project HealthSync.xcodeproj \
  -scheme HealthSync \
  -configuration Debug \
  -destination 'platform=iOS,name=Your iPhone' \
  build
```

## Common Failure Modes

### Signing Failure

Symptoms:

- provisioning profile errors
- missing signing certificate
- no team selected

Checks:

- set `DEVELOPMENT_TEAM` in [Config/Debug.xcconfig](/Users/robin/Desktop/aperture/Config/Debug.xcconfig)
- ensure the bundle identifier is unique for your team
- ensure your Apple account is available to Xcode on this machine

### Invalid Runtime Config

Symptoms:

- app launches and immediately reports setup failure
- network requests never start

Checks:

- `SERVER_BASE_URL` is valid HTTPS
- `HEALTH_API_KEY` matches the server

### HealthKit Authorization Failure

Symptoms:

- app installs but never begins syncing

Checks:

- device build, not simulator, for real HealthKit data
- HealthKit entitlement present
- usage description keys present

### Device Not Found

Symptoms:

- `xcodebuild` cannot match the destination
- `devicectl` cannot find the phone

Checks:

- unlock the phone
- trust the Mac on the device
- use the exact destination identifier

## Updating The App

For normal app code changes:

1. Edit Swift files or config.
2. Rebuild with `xcodebuild`.
3. Reinstall or rebuild directly to the device.

For project structure changes:

1. Edit [project.yml](/Users/robin/Desktop/aperture/project.yml).
2. Run `xcodegen generate`.
3. Rebuild.

## Current Status

As of now:

- the project spec exists
- the Xcode project has been generated
- simulator build succeeds from CLI
- the next required user-specific inputs are real config values and signing team configuration
