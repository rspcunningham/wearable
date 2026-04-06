import Foundation

struct BatchUploadCounts: Decodable {
    let records: Int
    let workouts: Int
    let profileSnapshots: Int
    let electrocardiograms: Int
    let workoutRoutes: Int
    let heartbeatSeries: Int
    let audiograms: Int
    let stateOfMind: Int

    enum CodingKeys: String, CodingKey {
        case records, workouts
        case profileSnapshots = "profile_snapshots"
        case electrocardiograms
        case workoutRoutes = "workout_routes"
        case heartbeatSeries = "heartbeat_series"
        case audiograms
        case stateOfMind = "state_of_mind"
    }
}

struct BatchUploadResponse: Decodable {
    let status: String
    let inserted: BatchUploadCounts
}

struct ServerInfoResponse: Decodable {
    let serverTime: String
    let lastIngestAt: String?
    let totalItems: Int
    let tables: [String: Int]

    enum CodingKeys: String, CodingKey {
        case serverTime = "server_time"
        case lastIngestAt = "last_ingest_at"
        case totalItems = "total_items"
        case tables
    }
}

enum BatchUploadResult {
    case skipped
    case success(BatchUploadResponse)
    case failure(String)
}

enum ServerInfoResult {
    case success(ServerInfoResponse)
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

    func fetchInfo() async -> ServerInfoResult {
        let config: AppConfig
        do {
            config = try loadConfig()
        } catch {
            let message = error.localizedDescription
            print("[ServerClient] Config error: \(message)")
            return .failure(message)
        }

        var request = URLRequest(url: config.baseURL.appending(path: "info"))
        request.httpMethod = "GET"
        request.setValue(config.apiKey, forHTTPHeaderField: "X-API-Key")
        request.timeoutInterval = 15

        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                return .failure("Server info request returned a non-HTTP response.")
            }

            guard http.statusCode == 200 else {
                let body = String(data: data, encoding: .utf8) ?? ""
                return .failure("Server info request failed with status \(http.statusCode): \(body)")
            }

            do {
                let decoded = try decoder.decode(ServerInfoResponse.self, from: data)
                return .success(decoded)
            } catch {
                print("[ServerClient] Decode error: \(error)")
                return .failure("Server info response could not be decoded.")
            }
        } catch {
            print("[ServerClient] Info request error: \(error)")
            return .failure(error.localizedDescription)
        }
    }
}
