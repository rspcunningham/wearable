import Foundation

struct PendingUploadItem: Codable, Identifiable {
    let id: UUID
    let source: String
    let payload: BatchPayload
    let enqueuedAt: Date

    init(id: UUID = UUID(), source: String, payload: BatchPayload, enqueuedAt: Date = Date()) {
        self.id = id
        self.source = source
        self.payload = payload
        self.enqueuedAt = enqueuedAt
    }
}

actor PendingUploadQueue {
    static let shared = PendingUploadQueue()

    typealias Uploader = @Sendable (PendingUploadItem) async -> Bool

    private let fileURL: URL
    private var items: [PendingUploadItem]

    init(fileURL: URL? = nil) {
        self.fileURL = fileURL ?? Self.defaultFileURL()
        self.items = Self.loadItems(from: self.fileURL)
    }

    func enqueue(payload: BatchPayload, source: String) throws {
        items.append(PendingUploadItem(source: source, payload: payload))
        try persist(items)
    }

    func flush(using uploader: Uploader) async throws -> Int {
        var flushed = 0

        while let next = items.first {
            let didUpload = await uploader(next)
            guard didUpload else { break }

            let updatedItems = Array(items.dropFirst())
            try persist(updatedItems)
            items = updatedItems
            flushed += 1
        }

        return flushed
    }

    func pendingCount() -> Int {
        items.count
    }

    private func persist(_ items: [PendingUploadItem]) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]

        let data = try encoder.encode(items)
        try Self.ensureDirectoryExists(for: fileURL)
        try data.write(to: fileURL, options: [.atomic])
    }

    private static func loadItems(from fileURL: URL) -> [PendingUploadItem] {
        guard let data = try? Data(contentsOf: fileURL) else { return [] }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return (try? decoder.decode([PendingUploadItem].self, from: data)) ?? []
    }

    private static func defaultFileURL() -> URL {
        let baseURL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return baseURL
            .appendingPathComponent(Bundle.main.bundleIdentifier ?? "HealthSync", isDirectory: true)
            .appendingPathComponent("pending_upload_queue.json")
    }

    private static func ensureDirectoryExists(for fileURL: URL) throws {
        let directoryURL = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
    }
}
