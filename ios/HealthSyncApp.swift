import SwiftUI
import HealthKit

@main
struct HealthSyncApp: App {
    var body: some Scene {
        WindowGroup {
            TestView()
        }
    }
}

struct TestView: View {
    @State private var lines: [String] = []
    @State private var running = false

    var body: some View {
        VStack(spacing: 16) {
            Text("Health Sync Test").font(.headline)

            ScrollView {
                VStack(alignment: .leading, spacing: 4) {
                    ForEach(Array(lines.enumerated()), id: \.offset) { _, line in
                        Text(line)
                            .font(.system(.caption, design: .monospaced))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            Button(running ? "Running..." : "Run Test") {
                guard !running else { return }
                running = true
                lines = []
                Task {
                    await runTest()
                    running = false
                }
            }
            .buttonStyle(.bordered)
            .disabled(running)
        }
        .padding()
    }

    private func emit(_ msg: String) {
        lines.append(msg)
    }

    private func runTest() async {
        // 1. Config
        let config: AppConfig
        do {
            config = try AppConfig.load()
            emit("1. Config OK")
            emit("   URL: \(config.baseURL)")
            emit("   Key: \(String(config.apiKey.prefix(4)))...")
        } catch {
            emit("1. Config FAILED: \(error.localizedDescription)")
            return
        }

        // 2. Server reachable?
        emit("2. Testing server...")
        do {
            let url = config.baseURL.appending(path: "health")
            var req = URLRequest(url: url)
            req.httpMethod = "GET"
            req.timeoutInterval = 10
            let (data, response) = try await URLSession.shared.data(for: req)
            let status = (response as? HTTPURLResponse)?.statusCode ?? -1
            let body = String(data: data, encoding: .utf8) ?? "n/a"
            emit("   Status: \(status)")
            emit("   Body: \(body)")
            guard status == 200 else {
                emit("2. FAILED: non-200 status")
                return
            }
        } catch {
            emit("2. Server FAILED: \(error.localizedDescription)")
            return
        }

        // 3. HealthKit auth
        let store = HKHealthStore()
        let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount)!
        do {
            try await store.requestAuthorization(toShare: [], read: [stepType])
            emit("3. HealthKit auth OK")
        } catch {
            emit("3. HealthKit FAILED: \(error.localizedDescription)")
            return
        }

        // 4. Query step count (last 7 days, max 100)
        let start = Calendar.current.date(byAdding: .day, value: -7, to: Date())!
        let predicate = HKQuery.predicateForSamples(withStart: start, end: nil)

        let samples: [HKQuantitySample]? = await withCheckedContinuation { cont in
            let q = HKSampleQuery(
                sampleType: stepType,
                predicate: predicate,
                limit: 100,
                sortDescriptors: [NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: false)]
            ) { _, results, error in
                if let error {
                    cont.resume(returning: nil)
                    return
                }
                cont.resume(returning: (results as? [HKQuantitySample]) ?? [])
            }
            store.execute(q)
        }

        guard let samples else {
            emit("4. Query FAILED")
            return
        }
        emit("4. Query OK: \(samples.count) step samples")

        if samples.isEmpty {
            emit("   No step data in last 7 days — nothing to upload")
            return
        }

        // Show a sample
        let first = samples[0]
        emit("   e.g. \(first.quantity.doubleValue(for: .count())) steps @ \(first.startDate)")

        // 5. Serialize
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        var payload = BatchPayload()
        for s in samples {
            payload.records.append(HealthRecord(
                sampleUUID: s.uuid.uuidString,
                recordType: s.quantityType.identifier,
                value: s.quantity.doubleValue(for: .count()),
                unit: "count",
                startDate: iso.string(from: s.startDate),
                endDate: iso.string(from: s.endDate),
                device: s.device?.name,
                sourceName: s.sourceRevision.source.name,
                metadata: nil
            ))
        }
        emit("5. Serialized \(payload.records.count) records")

        // 6. POST
        let ingestURL = config.baseURL.appending(path: "ingest")
        emit("6. POSTing to \(ingestURL)...")
        let result = await ServerClient.shared.postBatch(payload)
        switch result {
        case .skipped:
            emit("   Skipped (empty payload)")
        case .success(let response):
            emit("   SUCCESS: \(response.inserted.records) records inserted")
        case .failure(let msg):
            emit("   FAILED: \(msg)")
        }
    }
}
