import SwiftUI
import HealthKit

@main
struct HealthSyncApp: App {
    @StateObject private var hkManager = HealthKitManager.shared

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(hkManager)
                .task {
                    await setup()
                }
        }
    }

    func setup() async {
        do {
            _ = try ServerClient.shared.loadConfig()
            try await hkManager.requestAuthorization()
            _ = await hkManager.syncProfileSnapshot()
            hkManager.enableBackgroundDelivery()
            hkManager.startObservers()
            Task { _ = await hkManager.requestQueueFlush(reason: "launch") }

            // First launch: sync all historical data
            let hasSeeded = UserDefaults.standard.bool(forKey: "has_seeded_v1")
            if !hasSeeded {
                let didSeed = await hkManager.syncAllHistoricalData()
                if didSeed {
                    UserDefaults.standard.set(true, forKey: "has_seeded_v1")
                }
            }
        } catch {
            hkManager.syncStatus = "Setup failed"
            print("[Setup] Auth error: \(error)")
        }
    }
}


struct ContentView: View {
    @EnvironmentObject var hkManager: HealthKitManager

    var body: some View {
        VStack(spacing: 24) {
            Image(systemName: "heart.fill")
                .font(.system(size: 48))
                .foregroundColor(.red)

            Text("Health Sync")
                .font(.title).bold()

            StatusRow(label: "Status", value: hkManager.syncStatus)

            if let last = hkManager.lastSyncDate {
                StatusRow(label: "Last sync", value: last.formatted())
            }

            Text("This app runs in the background and continuously syncs your Apple Health data to your server.")
                .font(.caption)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Button("Sync Now") {
                Task {
                    _ = await hkManager.syncProfileSnapshot(force: true)
                    _ = await hkManager.syncAllHistoricalData()
                }
            }
            .buttonStyle(.bordered)
        }
        .padding()
    }
}

struct StatusRow: View {
    let label: String
    let value: String
    var body: some View {
        HStack {
            Text(label).foregroundColor(.secondary)
            Spacer()
            Text(value).bold()
        }
        .padding(.horizontal)
    }
}
