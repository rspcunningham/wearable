import Foundation

struct BatchUploadCounts: Decodable {
    let records: Int
    let workouts: Int
    let activitySummaries: Int
    let profileSnapshots: Int
    let electrocardiograms: Int
    let workoutRoutes: Int
    let heartbeatSeries: Int
    let audiograms: Int
    let stateOfMind: Int
    let correlations: Int

    enum CodingKeys: String, CodingKey {
        case records, workouts
        case activitySummaries = "activity_summaries"
        case profileSnapshots = "profile_snapshots"
        case electrocardiograms
        case workoutRoutes = "workout_routes"
        case heartbeatSeries = "heartbeat_series"
        case audiograms
        case stateOfMind = "state_of_mind"
        case correlations
    }
}

struct BatchUploadResponse: Decodable {
    let status: String
    let inserted: BatchUploadCounts
}

enum BatchUploadResult {
    case skipped
    case success(BatchUploadResponse)
    case failure(String)
}

class ServerClient {
    static let shared = ServerClient()

    private let encoder: JSONEncoder = {
        let e = JSONEncoder()
        e.outputFormatting = .prettyPrinted
        return e
    }()

    private let decoder = JSONDecoder()

    func loadConfig() throws -> AppConfig {
        try AppConfig.load()
    }

    func postBatch(_ payload: BatchPayload) async -> BatchUploadResult {
        guard !payload.isEmpty else { return .skipped }

        let config: AppConfig
        do {
            config = try loadConfig()
        } catch {
            let message = error.localizedDescription
            print("[ServerClient] Config error: \(message)")
            return .failure(message)
        }

        var request = URLRequest(url: config.baseURL.appending(path: "ingest"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(config.apiKey, forHTTPHeaderField: "X-API-Key")
        request.timeoutInterval = 30

        do {
            request.httpBody = try encoder.encode(payload)
        } catch {
            print("[ServerClient] Encode error: \(error)")
            return .failure("Failed to encode upload payload.")
        }

        // Simple retry with exponential backoff
        var lastFailure = "Upload failed."
        for attempt in 1...3 {
            do {
                let (data, response) = try await URLSession.shared.data(for: request)
                if let http = response as? HTTPURLResponse, http.statusCode == 200 {
                    let body = String(data: data, encoding: .utf8) ?? ""
                    let decoded = try? decoder.decode(BatchUploadResponse.self, from: data)
                    print("[ServerClient] Posted batch. Server: \(body)")
                    if let decoded {
                        return .success(decoded)
                    }
                    return .failure("Server returned success but response could not be decoded.")
                } else {
                    let body = String(data: data, encoding: .utf8) ?? ""
                    lastFailure = "Server rejected batch on attempt \(attempt): \(body)"
                    print("[ServerClient] Attempt \(attempt) failed. Response: \(body)")
                }
            } catch {
                lastFailure = "Attempt \(attempt) error: \(error.localizedDescription)"
                print("[ServerClient] Attempt \(attempt) error: \(error)")
            }
            if attempt < 3 {
                try? await Task.sleep(nanoseconds: UInt64(pow(2.0, Double(attempt))) * 1_000_000_000)
            }
        }

        print("[ServerClient] Failed to post batch after 3 attempts.")
        return .failure(lastFailure)
    }
}
