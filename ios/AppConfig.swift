import Foundation

enum AppConfigError: LocalizedError {
    case missingKey(String)
    case emptyValue(String)
    case invalidURL(String)

    var errorDescription: String? {
        switch self {
        case .missingKey(let key):
            return "Missing Info.plist value for \(key)."
        case .emptyValue(let key):
            return "Empty Info.plist value for \(key)."
        case .invalidURL(let value):
            return "Invalid server URL: \(value)"
        }
    }
}

struct AppConfig {
    let baseURL: URL
    let apiKey: String

    static func load(from bundle: Bundle = .main) throws -> AppConfig {
        let baseURLString = try stringValue(forKey: "SERVER_BASE_URL", in: bundle)
        let apiKey = try stringValue(forKey: "HEALTH_API_KEY", in: bundle)

        guard let baseURL = URL(string: baseURLString), baseURL.scheme != nil, baseURL.host != nil else {
            throw AppConfigError.invalidURL(baseURLString)
        }

        return AppConfig(baseURL: baseURL, apiKey: apiKey)
    }

    private static func stringValue(forKey key: String, in bundle: Bundle) throws -> String {
        guard let rawValue = bundle.object(forInfoDictionaryKey: key) as? String else {
            throw AppConfigError.missingKey(key)
        }

        let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty else {
            throw AppConfigError.emptyValue(key)
        }

        return value
    }
}
