import SwiftUI
import HealthKit
import UIKit

enum SyncDisplayPhase: String {
    case idle
    case debouncing
    case syncing
    case deferredProtectedData
    case failed

    var description: String {
        switch self {
        case .idle:
            return "Idle"
        case .debouncing:
            return "Debouncing"
        case .syncing:
            return "Syncing"
        case .deferredProtectedData:
            return "Deferred: protected data unavailable"
        case .failed:
            return "Failed"
        }
    }
}

enum SyncRunResultKind {
    case success
    case failed
    case aborted

    var description: String {
        switch self {
        case .success:
            return "Success"
        case .failed:
            return "Failed"
        case .aborted:
            return "Aborted"
        }
    }
}

struct LastRunSnapshot {
    let finishedAt: Date
    let result: SyncRunResultKind
    let uploadedSamples: Int
    let uploadedRequests: Int
    let typesEmpty: Int
    let queryFailures: Int
    let uploadFailures: Int
    let elapsedDescription: String
}

struct ServerInfoSnapshot {
    let serverTime: String
    let lastIngestAt: String?
    let totalItems: Int
    let tables: [String: Int]
}

@MainActor
final class SyncUIModel: ObservableObject {
    static let shared = SyncUIModel()

    @Published var lines: [String] = []
    @Published var isRunning = false
    @Published var phase: SyncDisplayPhase = .idle
    @Published var pendingSync = false
    @Published var hasStoredAnchors = false
    @Published var protectedDataAvailable = true
    @Published var lastTriggerBatchDescription: String?
    @Published var observerStatusDescription = "Unknown"
    @Published var observerRegisteredCount = 0
    @Published var observerBackgroundDeliveryCount = 0
    @Published var lastRun: LastRunSnapshot?
    @Published var isRefreshingServerInfo = false
    @Published var serverInfoError: String?
    @Published var lastServerInfoRefreshAt: Date?
    @Published var serverInfo: ServerInfoSnapshot?

    private let maxLogLines = 500

    private init() {}

    func append(_ line: String) {
        lines.append(line)
        if lines.count > maxLogLines {
            lines.removeFirst(lines.count - maxLogLines)
        }
    }

    func setRunning(_ running: Bool) {
        isRunning = running
    }

    func updateLocalState(
        phase: SyncDisplayPhase? = nil,
        pendingSync: Bool? = nil,
        hasStoredAnchors: Bool? = nil,
        protectedDataAvailable: Bool? = nil,
        lastTriggerBatchDescription: String? = nil
    ) {
        if let phase {
            self.phase = phase
        }
        if let pendingSync {
            self.pendingSync = pendingSync
        }
        if let hasStoredAnchors {
            self.hasStoredAnchors = hasStoredAnchors
        }
        if let protectedDataAvailable {
            self.protectedDataAvailable = protectedDataAvailable
        }
        if let lastTriggerBatchDescription {
            self.lastTriggerBatchDescription = lastTriggerBatchDescription
        }
    }

    func setObserverStatus(
        description: String,
        registeredCount: Int,
        backgroundDeliveryCount: Int
    ) {
        observerStatusDescription = description
        observerRegisteredCount = registeredCount
        observerBackgroundDeliveryCount = backgroundDeliveryCount
    }

    func setLastRun(_ snapshot: LastRunSnapshot) {
        lastRun = snapshot
    }

    func beginServerInfoRefresh() {
        isRefreshingServerInfo = true
        serverInfoError = nil
    }

    func finishServerInfoRefresh(
        info: ServerInfoSnapshot? = nil,
        error: String? = nil
    ) {
        isRefreshingServerInfo = false
        lastServerInfoRefreshAt = Date()
        if let info {
            serverInfo = info
            serverInfoError = nil
        } else {
            serverInfoError = error
        }
    }
}

private func makeSyncLogger() -> @Sendable (String) async -> Void {
    { message in
        await SyncUIModel.shared.append(message)
    }
}

final class HealthSyncAppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        let logger = makeSyncLogger()
        Task {
            await SyncCoordinator.shared.refreshDisplayedState()
            await HealthKitObserverManager.shared.startIfAuthorized(log: logger)
            await ServerInfoCoordinator.shared.refresh()
        }
        return true
    }

    func applicationDidBecomeActive(_ application: UIApplication) {
        let logger = makeSyncLogger()
        Task {
            await SyncCoordinator.shared.refreshDisplayedState()
            await SyncCoordinator.shared.handleTrigger(reason: .appDidBecomeActive, log: logger)
            await HealthKitObserverManager.shared.startIfAuthorized(log: logger)
            await ServerInfoCoordinator.shared.refresh()
        }
    }

    func applicationProtectedDataDidBecomeAvailable(_ application: UIApplication) {
        let logger = makeSyncLogger()
        Task {
            await SyncCoordinator.shared.refreshDisplayedState()
            await SyncCoordinator.shared.handleTrigger(reason: .protectedDataAvailable, log: logger)
        }
    }
}

@main
struct HealthSyncApp: App {
    @UIApplicationDelegateAdaptor(HealthSyncAppDelegate.self) private var appDelegate
    @StateObject private var uiModel = SyncUIModel.shared

    var body: some Scene {
        WindowGroup {
            AppView(uiModel: uiModel)
        }
    }
}

private struct SyncHistoryFilter: @unchecked Sendable {
    let predicate: NSPredicate?
    let description: String
}

private struct SyncEngineConfig: @unchecked Sendable {
    let workerCount: Int
    let hkPageSize: Int
    let queueCapacity: Int
    let uploadChunkSampleCount: Int
    let historyFilter: SyncHistoryFilter
}

private enum SyncDefaults {
    static let workerCount = 3
    static let hkPageSize = 1000
    static let queueCapacity = 6
    static let uploadChunkSampleCount = 2000

    static let historyFilter = SyncHistoryFilter(
        predicate: nil,
        description: "full history"
    )

    static let engineConfig = SyncEngineConfig(
        workerCount: workerCount,
        hkPageSize: hkPageSize,
        queueCapacity: queueCapacity,
        uploadChunkSampleCount: uploadChunkSampleCount,
        historyFilter: historyFilter
    )
}

private enum SyncTriggerReason: Hashable {
    case manual
    case observer
    case appDidBecomeActive
    case protectedDataAvailable
    case followUp

    var description: String {
        switch self {
        case .manual:
            return "manual"
        case .observer:
            return "observer"
        case .appDidBecomeActive:
            return "app became active"
        case .protectedDataAvailable:
            return "protected data became available"
        case .followUp:
            return "follow-up"
        }
    }

    var requiresPendingSync: Bool {
        switch self {
        case .manual, .observer, .followUp:
            return false
        case .appDidBecomeActive, .protectedDataAvailable:
            return true
        }
    }

    var backgroundTaskName: String {
        switch self {
        case .manual:
            return "HealthSyncManualSync"
        case .observer:
            return "HealthSyncObserverSync"
        case .appDidBecomeActive:
            return "HealthSyncActiveSync"
        case .protectedDataAvailable:
            return "HealthSyncProtectedDataSync"
        case .followUp:
            return "HealthSyncFollowUpSync"
        }
    }
}

private enum SyncPipelineResult: Equatable {
    case succeeded
    case failed
}

private struct SyncPipelineOutcome {
    let result: SyncPipelineResult
    let summary: SyncRunSummary
}

private struct SyncTypeWorkItem {
    let index: Int
    let totalCount: Int
    let type: HKSampleType
    let name: String
}

private enum SyncTypeRunResult {
    case completed
    case empty
    case failed
    case aborted
}

private struct SyncUploadJob {
    let payload: BatchPayload
    let typeIdentifier: String
    let nextAnchor: HKQueryAnchor?
}

private struct SyncUploadSummary {
    var uploadedSamples = 0
    var uploadedRequests = 0
    var failedUploads = 0
    var aborted = false
}

private struct SyncRunSummary {
    let typesEmpty: Int
    let queryFailures: Int
    let uploadedSamples: Int
    let uploadedRequests: Int
    let failedUploads: Int
    let aborted: Bool
    let elapsedDescription: String
}

private actor SyncPayloadQueue {
    enum PushResult {
        case enqueued(Int)
        case aborted
    }

    private let capacity: Int
    private var buffer: [SyncUploadJob] = []
    private var waitingProducerContinuations: [CheckedContinuation<Void, Never>] = []
    private var waitingConsumerContinuations: [CheckedContinuation<(SyncUploadJob, Int)?, Never>] = []
    private var isClosed = false
    private var isAborted = false

    init(capacity: Int) {
        self.capacity = capacity
    }

    func push(_ job: SyncUploadJob) async -> PushResult {
        if isAborted {
            return .aborted
        }

        if let consumer = dequeueWaitingConsumer() {
            consumer.resume(returning: (job, buffer.count))
            return .enqueued(buffer.count)
        }

        while !isClosed && !isAborted && buffer.count >= capacity {
            await withCheckedContinuation { continuation in
                waitingProducerContinuations.append(continuation)
            }

            if isAborted {
                return .aborted
            }

            if let consumer = dequeueWaitingConsumer() {
                consumer.resume(returning: (job, buffer.count))
                return .enqueued(buffer.count)
            }
        }

        guard !isClosed && !isAborted else { return .aborted }

        buffer.append(job)
        return .enqueued(buffer.count)
    }

    func next() async -> (SyncUploadJob, Int)? {
        if !buffer.isEmpty {
            let job = buffer.removeFirst()
            resumeNextProducerIfPossible()
            return (job, buffer.count)
        }

        if isClosed || isAborted {
            return nil
        }

        return await withCheckedContinuation { continuation in
            waitingConsumerContinuations.append(continuation)
        }
    }

    func finish() {
        isClosed = true

        let consumers = waitingConsumerContinuations
        waitingConsumerContinuations.removeAll()
        for consumer in consumers {
            consumer.resume(returning: nil)
        }

        let producers = waitingProducerContinuations
        waitingProducerContinuations.removeAll()
        for producer in producers {
            producer.resume()
        }
    }

    func abort() {
        isAborted = true
        isClosed = true
        buffer.removeAll()

        let consumers = waitingConsumerContinuations
        waitingConsumerContinuations.removeAll()
        for consumer in consumers {
            consumer.resume(returning: nil)
        }

        let producers = waitingProducerContinuations
        waitingProducerContinuations.removeAll()
        for producer in producers {
            producer.resume()
        }
    }

    func wasAborted() -> Bool {
        isAborted
    }

    private func dequeueWaitingConsumer() -> CheckedContinuation<(SyncUploadJob, Int)?, Never>? {
        guard !waitingConsumerContinuations.isEmpty else { return nil }
        return waitingConsumerContinuations.removeFirst()
    }

    private func resumeNextProducerIfPossible() {
        guard !isAborted, buffer.count < capacity, !waitingProducerContinuations.isEmpty else { return }
        let producer = waitingProducerContinuations.removeFirst()
        producer.resume()
    }
}

private actor SyncAnchorStore {
    static let shared = SyncAnchorStore()

    private let defaults = UserDefaults.standard
    private let keyPrefix = "hk_anchor_v1_"

    func loadAnchor(for typeIdentifier: String) -> HKQueryAnchor? {
        guard let data = defaults.data(forKey: key(for: typeIdentifier)) else { return nil }
        return try? NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data)
    }

    func saveAnchors(_ anchors: [String: HKQueryAnchor]) -> Int {
        var savedCount = 0

        for (typeIdentifier, anchor) in anchors {
            guard let data = try? NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true) else {
                continue
            }
            defaults.set(data, forKey: key(for: typeIdentifier))
            savedCount += 1
        }

        return savedCount
    }

    func clearAll() -> Int {
        let keys = defaults.dictionaryRepresentation().keys.filter { $0.hasPrefix(keyPrefix) }
        for key in keys {
            defaults.removeObject(forKey: key)
        }
        return keys.count
    }

    func hasAnyAnchors() -> Bool {
        defaults.dictionaryRepresentation().keys.contains { $0.hasPrefix(keyPrefix) }
    }

    private func key(for typeIdentifier: String) -> String {
        "\(keyPrefix)\(typeIdentifier)"
    }
}

private actor SyncStateStore {
    static let shared = SyncStateStore()

    private let defaults = UserDefaults.standard
    private let pendingSyncKey = "sync_pending_v1"

    func isPendingSync() -> Bool {
        defaults.bool(forKey: pendingSyncKey)
    }

    func setPendingSync(_ value: Bool) {
        defaults.set(value, forKey: pendingSyncKey)
    }
}

private enum SyncProtectedData {
    static func isAvailable() async -> Bool {
        await MainActor.run {
            UIApplication.shared.isProtectedDataAvailable
        }
    }
}

private enum SyncBackgroundExecution {
    static func begin(named name: String) async -> UIBackgroundTaskIdentifier {
        await MainActor.run {
            UIApplication.shared.beginBackgroundTask(withName: name) {
                print("[Sync] Background time expired for \(name)")
            }
        }
    }

    static func end(_ identifier: UIBackgroundTaskIdentifier) async {
        guard identifier != .invalid else { return }
        await MainActor.run {
            UIApplication.shared.endBackgroundTask(identifier)
        }
    }
}

private actor ServerInfoCoordinator {
    static let shared = ServerInfoCoordinator()

    private var isRefreshing = false

    func refresh() async {
        guard !isRefreshing else { return }
        isRefreshing = true
        await SyncUIModel.shared.beginServerInfoRefresh()

        let result = await ServerClient.shared.fetchInfo()
        switch result {
        case .success(let response):
            let snapshot = ServerInfoSnapshot(
                serverTime: response.serverTime,
                lastIngestAt: response.lastIngestAt,
                totalItems: response.totalItems,
                tables: response.tables
            )
            await SyncUIModel.shared.finishServerInfoRefresh(info: snapshot)
        case .failure(let message):
            await SyncUIModel.shared.finishServerInfoRefresh(error: message)
        }

        isRefreshing = false
    }
}

private actor HealthKitObserverManager {
    static let shared = HealthKitObserverManager()

    private let store = HKHealthStore()
    private var observerQueries: [String: HKObserverQuery] = [:]
    private var backgroundDeliveryEnabledIdentifiers: Set<String> = []
    private var didLogHealthDataUnavailable = false
    private var didLogAuthorizationUnavailable = false

    func startIfAuthorized(
        log: @escaping @Sendable (String) async -> Void
    ) async {
        guard HKHealthStore.isHealthDataAvailable() else {
            if !didLogHealthDataUnavailable {
                didLogHealthDataUnavailable = true
                await log("HealthKit observers unavailable: health data is not available on this device")
            }
            await SyncUIModel.shared.setObserverStatus(
                description: "Unavailable on this device",
                registeredCount: observerQueries.count,
                backgroundDeliveryCount: backgroundDeliveryEnabledIdentifiers.count
            )
            return
        }

        didLogHealthDataUnavailable = false

        let authorizationResult = await authorizationStatus()
        switch authorizationResult {
        case .failure(let error):
            await log("HealthKit observer setup failed: \(SyncEngine.describe(error: error))")
            await SyncUIModel.shared.setObserverStatus(
                description: "Setup error",
                registeredCount: observerQueries.count,
                backgroundDeliveryCount: backgroundDeliveryEnabledIdentifiers.count
            )
            return
        case .success(let status):
            guard status == .unnecessary else {
                if !didLogAuthorizationUnavailable {
                    didLogAuthorizationUnavailable = true
                    await log("HealthKit observers deferred: HealthKit authorization not granted yet")
                }
                await SyncUIModel.shared.setObserverStatus(
                    description: "Deferred: auth not granted",
                    registeredCount: observerQueries.count,
                    backgroundDeliveryCount: backgroundDeliveryEnabledIdentifiers.count
                )
                return
            }
        }

        didLogAuthorizationUnavailable = false

        let types = HKSync.allSampleTypes
        var registeredCount = 0
        var enabledCount = 0

        for sampleType in types {
            let typeIdentifier = sampleType.identifier

            if observerQueries[typeIdentifier] == nil {
                switch await registerObserver(for: sampleType) {
                case .success(let query):
                    observerQueries[typeIdentifier] = query
                    registeredCount += 1
                case .failure(let error):
                    await log("HealthKit observer register failed for \(SyncEngine.shortName(forIdentifier: typeIdentifier)): \(SyncEngine.describe(error: error))")
                }
            }

            if !backgroundDeliveryEnabledIdentifiers.contains(typeIdentifier) {
                switch await enableBackgroundDelivery(for: sampleType) {
                case .success:
                    backgroundDeliveryEnabledIdentifiers.insert(typeIdentifier)
                    enabledCount += 1
                case .failure(let error):
                    await log("HealthKit background delivery failed for \(SyncEngine.shortName(forIdentifier: typeIdentifier)): \(SyncEngine.describe(error: error))")
                }
            }
        }

        if registeredCount > 0 || enabledCount > 0 {
            await log("HealthKit observers active (\(observerQueries.count) types, registered \(registeredCount), background delivery enabled \(enabledCount), frequency immediate)")
        }
        await SyncUIModel.shared.setObserverStatus(
            description: "Active",
            registeredCount: observerQueries.count,
            backgroundDeliveryCount: backgroundDeliveryEnabledIdentifiers.count
        )
    }

    private func authorizationStatus() async -> Result<HKAuthorizationRequestStatus, Error> {
        await withCheckedContinuation { continuation in
            store.getRequestStatusForAuthorization(toShare: [], read: HKSync.allReadTypes) { status, error in
                if let error {
                    continuation.resume(returning: .failure(error))
                    return
                }
                continuation.resume(returning: .success(status))
            }
        }
    }

    private func registerObserver(
        for sampleType: HKSampleType
    ) async -> Result<HKObserverQuery, Error> {
        let query = HKObserverQuery(sampleType: sampleType, predicate: nil) { _, completionHandler, error in
            let logger = makeSyncLogger()

            if let error {
                Task {
                    await logger("HealthKit observer error for \(SyncEngine.shortName(forIdentifier: sampleType.identifier)): \(SyncEngine.describe(error: error))")
                }
                completionHandler()
                return
            }

            Task(priority: .background) {
                await SyncCoordinator.shared.handleTrigger(reason: .observer, log: logger)
            }
            completionHandler()
        }

        store.execute(query)
        return .success(query)
    }

    private func enableBackgroundDelivery(
        for sampleType: HKSampleType
    ) async -> Result<Void, Error> {
        await withCheckedContinuation { continuation in
            store.enableBackgroundDelivery(for: sampleType, frequency: .immediate) { success, error in
                if let error {
                    continuation.resume(returning: .failure(error))
                    return
                }

                guard success else {
                    let error = NSError(
                        domain: "HealthSyncObserverManager",
                        code: 1,
                        userInfo: [NSLocalizedDescriptionKey: "Background delivery was not enabled"]
                    )
                    continuation.resume(returning: .failure(error))
                    return
                }

                continuation.resume(returning: .success(()))
            }
        }
    }
}

private struct SyncEngine: Sendable {
    let config: SyncEngineConfig
    let anchorStore: SyncAnchorStore = .shared

    func run(
        store: HKHealthStore,
        types: [HKSampleType],
        log: @escaping @Sendable (String) async -> Void
    ) async -> SyncRunSummary {
        var typesEmpty = 0
        var queryFailures = 0
        let syncStartedAt = Date()

        let workItems = types.enumerated().map { index, type in
            SyncTypeWorkItem(index: index + 1, totalCount: types.count, type: type, name: Self.shortName(forIdentifier: type.identifier))
        }

        let payloadQueue = SyncPayloadQueue(capacity: config.queueCapacity)
        let uploaderTask = Task {
            await processUploads(from: payloadQueue, log: log)
        }

        var nextWorkIndex = 0
        await withTaskGroup(of: SyncTypeRunResult.self) { group in
            let initialWorkerCount = min(config.workerCount, workItems.count)
            for _ in 0..<initialWorkerCount {
                let workItem = workItems[nextWorkIndex]
                nextWorkIndex += 1
                group.addTask {
                    await processType(workItem, store: store, queue: payloadQueue, log: log)
                }
            }

            while let result = await group.next() {
                if nextWorkIndex < workItems.count {
                    let workItem = workItems[nextWorkIndex]
                    nextWorkIndex += 1
                    group.addTask {
                        await processType(workItem, store: store, queue: payloadQueue, log: log)
                    }
                }

                switch result {
                case .completed:
                    break
                case .empty:
                    typesEmpty += 1
                case .failed:
                    queryFailures += 1
                case .aborted:
                    break
                }
            }
        }

        await payloadQueue.finish()
        let uploadSummary = await uploaderTask.value

        return SyncRunSummary(
            typesEmpty: typesEmpty,
            queryFailures: queryFailures,
            uploadedSamples: uploadSummary.uploadedSamples,
            uploadedRequests: uploadSummary.uploadedRequests,
            failedUploads: uploadSummary.failedUploads,
            aborted: uploadSummary.aborted,
            elapsedDescription: Self.durationString(since: syncStartedAt)
        )
    }

    private func fetchAnchoredPage(
        store: HKHealthStore,
        type: HKSampleType,
        anchor: HKQueryAnchor?
    ) async -> Result<(samples: [HKSample], newAnchor: HKQueryAnchor?), Error> {
        await withCheckedContinuation { continuation in
            let query = HKAnchoredObjectQuery(
                type: type,
                predicate: config.historyFilter.predicate,
                anchor: anchor,
                limit: config.hkPageSize
            ) { _, samples, _, newAnchor, error in
                if let error {
                    continuation.resume(returning: .failure(error))
                    return
                }
                continuation.resume(returning: .success((samples: samples ?? [], newAnchor: newAnchor)))
            }
            store.execute(query)
        }
    }

    private func processType(
        _ workItem: SyncTypeWorkItem,
        store: HKHealthStore,
        queue: SyncPayloadQueue,
        log: @escaping @Sendable (String) async -> Void
    ) async -> SyncTypeRunResult {
        let startingAnchor = await anchorStore.loadAnchor(for: workItem.type.identifier)
        if startingAnchor != nil {
            await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): starting drain from saved anchor...")
        } else {
            await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): starting cold-start drain...")
        }

        let typeStartedAt = Date()
        var anchor = startingAnchor
        var page = 0
        var fetchedSamples = 0
        var enqueuedSamples = 0

        while true {
            if await queue.wasAborted() {
                return .aborted
            }

            page += 1
            let queryStartedAt = Date()
            let result = await fetchAnchoredPage(store: store, type: workItem.type, anchor: anchor)
            let queryDuration = Self.durationString(since: queryStartedAt)

            guard case .success(let result) = result else {
                let errorDescription: String
                if case .failure(let error) = result {
                    errorDescription = Self.describe(error: error)
                } else {
                    errorDescription = "Unknown query failure"
                }
                await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): QUERY ERROR on page \(page) after \(queryDuration) — \(errorDescription)")
                return .failed
            }

            let samples = result.samples
            anchor = result.newAnchor

            guard !samples.isEmpty else {
                if let anchor {
                    let job = SyncUploadJob(payload: BatchPayload(), typeIdentifier: workItem.type.identifier, nextAnchor: anchor)
                    switch await queue.push(job) {
                    case .enqueued(let queueDepth):
                        await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): queued anchor checkpoint (queue depth \(queueDepth))")
                    case .aborted:
                        return .aborted
                    }
                }

                if fetchedSamples == 0 {
                    await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): empty (\(queryDuration))")
                    return .empty
                }

                let totalDuration = Self.durationString(since: typeStartedAt)
                await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): complete (\(page - 1) pages, \(fetchedSamples) fetched, \(enqueuedSamples) queued, elapsed \(totalDuration))")
                return .completed
            }

            fetchedSamples += samples.count
            let cappedSuffix = samples.count == config.hkPageSize ? " (full page)" : ""
            await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): page \(page) fetched \(samples.count) samples\(cappedSuffix) in \(queryDuration); serializing...")

            let serializeStartedAt = Date()
            var payload = BatchPayload()
            for sample in samples {
                if let fragment = await HKSync.serialize(sample, store: store) {
                    payload.merge(fragment)
                }
            }
            let serializeDuration = Self.durationString(since: serializeStartedAt)

            if payload.isEmpty {
                if let anchor {
                    let job = SyncUploadJob(payload: BatchPayload(), typeIdentifier: workItem.type.identifier, nextAnchor: anchor)
                    switch await queue.push(job) {
                    case .enqueued(let queueDepth):
                        await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): page \(page) produced no serializable payload (\(serializeDuration)); queued anchor checkpoint (queue depth \(queueDepth))")
                    case .aborted:
                        return .aborted
                    }
                } else {
                    await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): page \(page) produced no serializable payload (\(serializeDuration))")
                }
            } else {
                let job = SyncUploadJob(payload: payload, typeIdentifier: workItem.type.identifier, nextAnchor: anchor)
                switch await queue.push(job) {
                case .enqueued(let queueDepth):
                    enqueuedSamples += payload.sampleCount
                    await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): page \(page) queued \(payload.sampleCount) serialized samples (serialize \(serializeDuration), queue depth \(queueDepth))")
                case .aborted:
                    return .aborted
                }
            }

            if samples.count < config.hkPageSize {
                let totalDuration = Self.durationString(since: typeStartedAt)
                await log("\(workItem.index)/\(workItem.totalCount) \(workItem.name): complete (\(page) pages, \(fetchedSamples) fetched, \(enqueuedSamples) queued, elapsed \(totalDuration))")
                return .completed
            }
        }
    }

    private func processUploads(
        from queue: SyncPayloadQueue,
        log: @escaping @Sendable (String) async -> Void
    ) async -> SyncUploadSummary {
        var summary = SyncUploadSummary()
        var accumulated = BatchPayload()
        var pendingAnchors: [String: HKQueryAnchor] = [:]

        while let (job, queueDepth) = await queue.next() {
            accumulated.merge(job.payload)
            if let nextAnchor = job.nextAnchor {
                pendingAnchors[job.typeIdentifier] = nextAnchor
            }

            if job.payload.isEmpty {
                await log("Uploader: queued anchor checkpoint for \(Self.shortName(forIdentifier: job.typeIdentifier)) (queue depth \(queueDepth), pending anchors \(pendingAnchors.count))")
            } else {
                await log("Uploader: merged \(job.payload.sampleCount) samples (accumulated \(accumulated.sampleCount), queue depth \(queueDepth), pending anchors \(pendingAnchors.count))")
            }

            if accumulated.sampleCount >= config.uploadChunkSampleCount {
                let didFlush = await flushAccumulatedPayload(&accumulated, pendingAnchors: &pendingAnchors, into: &summary, queue: queue, log: log)
                if !didFlush {
                    return summary
                }
            }
        }

        if !accumulated.isEmpty {
            let didFlush = await flushAccumulatedPayload(&accumulated, pendingAnchors: &pendingAnchors, into: &summary, queue: queue, log: log)
            if !didFlush {
                return summary
            }
        }

        if !pendingAnchors.isEmpty {
            let savedCount = await anchorStore.saveAnchors(pendingAnchors)
            await log("Uploader: saved \(savedCount) anchor-only updates")
        }

        return summary
    }

    private func flushAccumulatedPayload(
        _ payload: inout BatchPayload,
        pendingAnchors: inout [String: HKQueryAnchor],
        into summary: inout SyncUploadSummary,
        queue: SyncPayloadQueue,
        log: @escaping @Sendable (String) async -> Void
    ) async -> Bool {
        guard !payload.isEmpty else { return true }

        let sampleCount = payload.sampleCount
        await log("Uploader: posting \(sampleCount) samples...")

        let postStartedAt = Date()
        let postResult = await ServerClient.shared.postBatch(payload)
        let postDuration = Self.durationString(since: postStartedAt)

        switch postResult {
        case .skipped:
            await log("Uploader: skipped \(sampleCount) samples")
        case .success:
            summary.uploadedRequests += 1
            summary.uploadedSamples += sampleCount
            let savedCount = await anchorStore.saveAnchors(pendingAnchors)
            await log("Uploader: saved \(savedCount) anchors")
            await log("Uploader: OK (\(sampleCount) samples, post \(postDuration))")
        case .failure(let msg):
            summary.failedUploads += 1
            summary.aborted = true
            await log("Uploader: POST FAILED after \(postDuration) — \(msg)")
            await log("Uploader: aborting run; anchors not advanced for failed chunk")
            await queue.abort()
            return false
        }

        payload = BatchPayload()
        pendingAnchors.removeAll()
        return true
    }

    fileprivate static func shortName(forIdentifier id: String) -> String {
        for prefix in ["HKQuantityTypeIdentifier", "HKCategoryTypeIdentifier", "HKDataTypeIdentifier"] {
            if id.hasPrefix(prefix) { return String(id.dropFirst(prefix.count)) }
        }
        if id == "HKWorkoutTypeIdentifier" { return "Workout" }
        return id
    }

    fileprivate static func durationString(since start: Date) -> String {
        let ms = Int(Date().timeIntervalSince(start) * 1000)
        if ms < 1000 {
            return "\(ms)ms"
        }
        return String(format: "%.2fs", Double(ms) / 1000.0)
    }

    fileprivate static func describe(error: Error) -> String {
        let nsError = error as NSError
        var components = ["\(nsError.domain) code \(nsError.code)"]

        if let hkError = error as? HKError {
            components.append(String(describing: hkError.code))
        }

        let message = nsError.localizedDescription.trimmingCharacters(in: .whitespacesAndNewlines)
        if !message.isEmpty {
            components.append(message)
        }

        return components.joined(separator: " — ")
    }
}

private actor SyncCoordinator {
    static let shared = SyncCoordinator()

    private let stateStore = SyncStateStore.shared
    private let anchorStore = SyncAnchorStore.shared
    private let engine = SyncEngine(config: SyncDefaults.engineConfig)
    private let debounceDurationNanoseconds: UInt64 = 500_000_000

    private var isSyncRunning = false
    private var debounceTask: Task<Void, Never>?
    private var pendingTriggerCounts: [SyncTriggerReason: Int] = [:]
    private var didLogCoalescingWhileDebouncing = false
    private var didLogQueuedWhileRunning = false

    func refreshDisplayedState(
        phase: SyncDisplayPhase? = nil,
        lastTriggerBatchDescription: String? = nil
    ) async {
        let pendingSync = await stateStore.isPendingSync()
        let hasStoredAnchors = await anchorStore.hasAnyAnchors()
        let protectedDataAvailable = await SyncProtectedData.isAvailable()
        await SyncUIModel.shared.updateLocalState(
            phase: phase,
            pendingSync: pendingSync,
            hasStoredAnchors: hasStoredAnchors,
            protectedDataAvailable: protectedDataAvailable,
            lastTriggerBatchDescription: lastTriggerBatchDescription
        )
    }

    func handleTrigger(
        reason: SyncTriggerReason,
        log: @escaping @Sendable (String) async -> Void
    ) async {
        if reason.requiresPendingSync {
            let pendingSync = await stateStore.isPendingSync()
            guard pendingSync else { return }
        }

        await stateStore.setPendingSync(true)

        let hadNoPendingTriggers = pendingTriggerCounts.isEmpty
        pendingTriggerCounts[reason, default: 0] += 1
        let triggerBatchDescription = Self.describe(triggerCounts: pendingTriggerCounts)
        await refreshDisplayedState(
            phase: isSyncRunning ? nil : .debouncing,
            lastTriggerBatchDescription: triggerBatchDescription
        )

        if isSyncRunning {
            if hadNoPendingTriggers && !didLogQueuedWhileRunning {
                didLogQueuedWhileRunning = true
                await log("Sync signal received during active run; follow-up pass queued")
            }
            return
        }

        if debounceTask == nil {
            didLogCoalescingWhileDebouncing = false
            await log("Sync signaled (\(reason.description)); starting in 500ms unless more triggers arrive")
            debounceTask = Task { [self] in
                try? await Task.sleep(nanoseconds: debounceDurationNanoseconds)
                await fireDebouncedRun(log: log)
            }
            return
        }

        if !didLogCoalescingWhileDebouncing {
            didLogCoalescingWhileDebouncing = true
            await log("Additional sync signals received; coalescing into scheduled run")
        }
    }

    private func fireDebouncedRun(
        log: @escaping @Sendable (String) async -> Void
    ) async {
        debounceTask = nil
        didLogCoalescingWhileDebouncing = false

        guard !pendingTriggerCounts.isEmpty else { return }
        guard !isSyncRunning else { return }

        let triggerCounts = pendingTriggerCounts
        pendingTriggerCounts.removeAll()

        isSyncRunning = true
        didLogQueuedWhileRunning = false
        await SyncUIModel.shared.setRunning(true)

        let primaryReason = Self.primaryReason(from: triggerCounts)
        let triggerBatchDescription = Self.describe(triggerCounts: triggerCounts)
        await refreshDisplayedState(
            phase: .syncing,
            lastTriggerBatchDescription: triggerBatchDescription
        )
        await log("Sync trigger batch: \(triggerBatchDescription)")

        let protectedDataAvailable = await SyncProtectedData.isAvailable()
        guard protectedDataAvailable else {
            isSyncRunning = false
            await SyncUIModel.shared.setRunning(false)
            await stateStore.setPendingSync(true)
            await refreshDisplayedState(phase: .deferredProtectedData)
            await log("Sync deferred (\(primaryReason.description)): protected data unavailable; pending sync retained")
            return
        }

        let pipelineOutcome = await withBackgroundContinuation(
            named: primaryReason.backgroundTaskName,
            log: log
        ) { [self] in
            await self.runPipeline(log: log)
        }

        isSyncRunning = false
        await SyncUIModel.shared.setRunning(false)

        let lastRunResult: SyncRunResultKind = pipelineOutcome.summary.aborted
            ? .aborted
            : (pipelineOutcome.result == .succeeded ? .success : .failed)
        await SyncUIModel.shared.setLastRun(
            LastRunSnapshot(
                finishedAt: Date(),
                result: lastRunResult,
                uploadedSamples: pipelineOutcome.summary.uploadedSamples,
                uploadedRequests: pipelineOutcome.summary.uploadedRequests,
                typesEmpty: pipelineOutcome.summary.typesEmpty,
                queryFailures: pipelineOutcome.summary.queryFailures,
                uploadFailures: pipelineOutcome.summary.failedUploads,
                elapsedDescription: pipelineOutcome.summary.elapsedDescription
            )
        )

        switch pipelineOutcome.result {
        case .succeeded:
            if pendingTriggerCounts.isEmpty {
                await stateStore.setPendingSync(false)
                await refreshDisplayedState(phase: .idle)
                await log("Sync succeeded; pending sync cleared")
                return
            }

            await stateStore.setPendingSync(true)
            await refreshDisplayedState(
                phase: .debouncing,
                lastTriggerBatchDescription: Self.describe(triggerCounts: pendingTriggerCounts)
            )
            await log("Sync succeeded; more signals arrived during the run (\(Self.describe(triggerCounts: pendingTriggerCounts))); scheduling follow-up")
            await scheduleFollowUpDebounce(log: log)

        case .failed:
            pendingTriggerCounts.removeAll()
            didLogCoalescingWhileDebouncing = false
            await stateStore.setPendingSync(true)
            await refreshDisplayedState(phase: .failed)
            await log("Sync failed; pending sync retained for retry")
        }
    }

    private func scheduleFollowUpDebounce(
        log: @escaping @Sendable (String) async -> Void
    ) async {
        guard debounceTask == nil, !pendingTriggerCounts.isEmpty else { return }
        didLogCoalescingWhileDebouncing = false
        debounceTask = Task { [self] in
            try? await Task.sleep(nanoseconds: debounceDurationNanoseconds)
            await fireDebouncedRun(log: log)
        }
    }

    private func withBackgroundContinuation<T>(
        named name: String,
        log: @escaping @Sendable (String) async -> Void,
        operation: @escaping () async -> T
    ) async -> T {
        let backgroundTaskID = await SyncBackgroundExecution.begin(named: name)
        if backgroundTaskID != .invalid {
            await log("Background continuation armed")
        } else {
            await log("Background continuation unavailable")
        }

        let result = await operation()
        await SyncBackgroundExecution.end(backgroundTaskID)
        return result
    }

    private func runPipeline(
        log: @escaping @Sendable (String) async -> Void
    ) async -> SyncPipelineOutcome {
        let pipelineStartedAt = Date()
        let store = HKHealthStore()

        let appConfig: AppConfig
        do {
            appConfig = try AppConfig.load()
            await log("1. Config OK")
            await log("URL: \(appConfig.baseURL)")
            await log("Key: \(String(appConfig.apiKey.prefix(4)))...")
        } catch {
            await log("1. Config FAILED: \(error.localizedDescription)")
            return makePipelineOutcome(
                result: .failed,
                startedAt: pipelineStartedAt
            )
        }

        await log("2. Testing server...")
        do {
            let url = appConfig.baseURL.appending(path: "health")
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            request.timeoutInterval = 10
            let (data, response) = try await URLSession.shared.data(for: request)
            let status = (response as? HTTPURLResponse)?.statusCode ?? -1
            let body = String(data: data, encoding: .utf8) ?? "n/a"
            await log("Status: \(status) — \(body)")
            guard status == 200 else {
                await log("2. FAILED: non-200 status")
                return makePipelineOutcome(
                    result: .failed,
                    startedAt: pipelineStartedAt
                )
            }
        } catch {
            await log("2. Server FAILED: \(error.localizedDescription)")
            return makePipelineOutcome(
                result: .failed,
                startedAt: pipelineStartedAt
            )
        }

        do {
            try await store.requestAuthorization(toShare: [], read: HKSync.allReadTypes)
            await log("3. HealthKit auth OK")
            await HealthKitObserverManager.shared.startIfAuthorized(log: log)
        } catch {
            await log("3. HealthKit FAILED: \(error.localizedDescription)")
            return makePipelineOutcome(
                result: .failed,
                startedAt: pipelineStartedAt
            )
        }

        let types = HKSync.allSampleTypes
        let hasStoredAnchors = await anchorStore.hasAnyAnchors()
        let modeDescription = hasStoredAnchors ? "incremental" : "cold start"
        let config = engine.config

        await log("4. Syncing \(types.count) types (\(modeDescription), \(config.historyFilter.description), HK page \(config.hkPageSize), upload chunk \(config.uploadChunkSampleCount), queue \(config.queueCapacity), \(config.workerCount) workers)...")

        let summary = await engine.run(store: store, types: types, log: log)
        let succeeded = !summary.aborted && summary.queryFailures == 0 && summary.failedUploads == 0

        await log("")
        if summary.aborted {
            await log("Aborted: \(summary.uploadedSamples) samples uploaded in \(summary.uploadedRequests) requests, \(summary.typesEmpty) empty types, \(summary.queryFailures) query failures, \(summary.failedUploads) upload failures, elapsed \(summary.elapsedDescription)")
        } else if !succeeded {
            await log("Failed: \(summary.uploadedSamples) samples uploaded in \(summary.uploadedRequests) requests, \(summary.typesEmpty) empty types, \(summary.queryFailures) query failures, \(summary.failedUploads) upload failures, elapsed \(summary.elapsedDescription)")
        } else {
            await log("Done: \(summary.uploadedSamples) samples uploaded in \(summary.uploadedRequests) requests, \(summary.typesEmpty) empty types, \(summary.queryFailures) query failures, \(summary.failedUploads) upload failures, elapsed \(summary.elapsedDescription)")
        }

        return SyncPipelineOutcome(
            result: succeeded ? .succeeded : .failed,
            summary: summary
        )
    }

    private func makePipelineOutcome(
        result: SyncPipelineResult,
        startedAt: Date
    ) -> SyncPipelineOutcome {
        SyncPipelineOutcome(
            result: result,
            summary: SyncRunSummary(
                typesEmpty: 0,
                queryFailures: 0,
                uploadedSamples: 0,
                uploadedRequests: 0,
                failedUploads: 0,
                aborted: false,
                elapsedDescription: SyncEngine.durationString(since: startedAt)
            )
        )
    }

    private static func describe(triggerCounts: [SyncTriggerReason: Int]) -> String {
        let orderedReasons: [SyncTriggerReason] = [.manual, .observer, .appDidBecomeActive, .protectedDataAvailable, .followUp]
        let parts = orderedReasons.compactMap { reason -> String? in
            guard let count = triggerCounts[reason], count > 0 else { return nil }
            return count == 1 ? reason.description : "\(reason.description) x\(count)"
        }
        return parts.joined(separator: ", ")
    }

    private static func primaryReason(from triggerCounts: [SyncTriggerReason: Int]) -> SyncTriggerReason {
        let orderedReasons: [SyncTriggerReason] = [.manual, .observer, .appDidBecomeActive, .protectedDataAvailable, .followUp]
        for reason in orderedReasons where triggerCounts[reason] != nil {
            return reason
        }
        return .followUp
    }
}

private struct InfoRowView: View {
    let label: String
    let value: String

    var body: some View {
        HStack(alignment: .firstTextBaseline) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
            Spacer(minLength: 12)
            Text(value)
                .font(.caption.monospaced())
                .multilineTextAlignment(.trailing)
        }
    }
}

private struct InfoCardView: View {
    let title: String
    let rows: [(String, String)]

    var body: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 8) {
                ForEach(Array(rows.enumerated()), id: \.offset) { _, row in
                    InfoRowView(label: row.0, value: row.1)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        } label: {
            Text(title)
                .font(.subheadline.weight(.semibold))
        }
    }
}

struct AppView: View {
    @ObservedObject var uiModel: SyncUIModel

    private var isBusy: Bool {
        uiModel.phase == .debouncing || uiModel.isRunning
    }

    private var syncButtonTitle: String {
        switch uiModel.phase {
        case .debouncing:
            return "Queued..."
        case .syncing:
            return "Syncing..."
        default:
            return "Sync All"
        }
    }

    private var statusRows: [(String, String)] {
        [
            ("Phase", uiModel.phase.description),
            ("Pending", uiModel.pendingSync ? "Yes" : "No"),
            ("Mode", uiModel.hasStoredAnchors ? "Incremental" : "Cold start"),
            ("Protected Data", uiModel.protectedDataAvailable ? "Available" : "Unavailable"),
            ("Last Trigger", uiModel.lastTriggerBatchDescription ?? "None"),
        ]
    }

    private var observerRows: [(String, String)] {
        [
            ("Status", uiModel.observerStatusDescription),
            ("Registered", "\(uiModel.observerRegisteredCount)"),
            ("Background Delivery", "\(uiModel.observerBackgroundDeliveryCount)"),
        ]
    }

    private var lastRunRows: [(String, String)] {
        guard let lastRun = uiModel.lastRun else {
            return [("Last Run", "None yet")]
        }

        return [
            ("Result", lastRun.result.description),
            ("Finished", Self.formatTimestamp(lastRun.finishedAt)),
            ("Uploaded Samples", "\(lastRun.uploadedSamples)"),
            ("Requests", "\(lastRun.uploadedRequests)"),
            ("Empty Types", "\(lastRun.typesEmpty)"),
            ("Query Failures", "\(lastRun.queryFailures)"),
            ("Upload Failures", "\(lastRun.uploadFailures)"),
            ("Elapsed", lastRun.elapsedDescription),
        ]
    }

    private var serverRows: [(String, String)] {
        var rows: [(String, String)] = [
            ("Refreshing", uiModel.isRefreshingServerInfo ? "Yes" : "No"),
            ("Last Refresh", uiModel.lastServerInfoRefreshAt.map(Self.formatTimestamp) ?? "Never"),
        ]

        if let error = uiModel.serverInfoError {
            rows.append(("Error", error))
        }

        if let serverInfo = uiModel.serverInfo {
            rows.append(("Server Time", Self.formatTimestamp(serverInfo.serverTime) ?? serverInfo.serverTime))
            rows.append(("Last Ingest", Self.formatTimestamp(serverInfo.lastIngestAt) ?? "Never"))
            rows.append(("Total Items", "\(serverInfo.totalItems)"))

            let nonzeroTables = Self.orderedServerTables.compactMap { tableName -> (String, String)? in
                let count = serverInfo.tables[tableName] ?? 0
                guard count > 0 else { return nil }
                return (Self.displayName(forServerTable: tableName), "\(count)")
            }

            if nonzeroTables.isEmpty {
                rows.append(("Tables", "No rows yet"))
            } else {
                rows.append(contentsOf: nonzeroTables)
            }
        } else {
            rows.append(("Server", "No snapshot yet"))
        }

        return rows
    }

    var body: some View {
        TabView {
            dashboardTab
                .tabItem { Label("Dashboard", systemImage: "heart.text.square") }

            logsTab
                .tabItem { Label("Logs", systemImage: "text.justify.left") }
        }
        .task {
            await SyncCoordinator.shared.refreshDisplayedState()
            await ServerInfoCoordinator.shared.refresh()
        }
    }

    private var dashboardTab: some View {
        ScrollView {
            VStack(spacing: 16) {
                Text("Health Sync").font(.headline)

                VStack(spacing: 12) {
                    InfoCardView(title: "Status", rows: statusRows)
                    InfoCardView(title: "Observers", rows: observerRows)
                    InfoCardView(title: "Last Run", rows: lastRunRows)
                    InfoCardView(title: "Server", rows: serverRows)
                }

                HStack(spacing: 12) {
                    Button(syncButtonTitle) {
                        guard !isBusy else { return }
                        let logger = makeSyncLogger()
                        Task {
                            await SyncCoordinator.shared.handleTrigger(reason: .manual, log: logger)
                        }
                    }
                    .buttonStyle(.bordered)
                    .disabled(isBusy)

                    Button(uiModel.isRefreshingServerInfo ? "Refreshing..." : "Refresh Server") {
                        guard !uiModel.isRefreshingServerInfo else { return }
                        Task {
                            await ServerInfoCoordinator.shared.refresh()
                        }
                    }
                    .buttonStyle(.bordered)
                    .disabled(uiModel.isRefreshingServerInfo)

                    Button("Clear Anchors") {
                        guard !isBusy else { return }
                        Task {
                            let clearedCount = await SyncAnchorStore.shared.clearAll()
                            await SyncCoordinator.shared.refreshDisplayedState()
                            SyncUIModel.shared.append("Anchors cleared (\(clearedCount) entries)")
                        }
                    }
                    .buttonStyle(.bordered)
                    .disabled(isBusy)
                }
            }
            .padding()
        }
    }

    private var logsTab: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 4) {
                    ForEach(Array(uiModel.lines.enumerated()), id: \.offset) { _, line in
                        Text(line)
                            .font(.system(.caption, design: .monospaced))
                    }

                    Color.clear
                        .frame(height: 1)
                        .id("log-bottom")
                }
                .padding()
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .onChange(of: uiModel.lines.count) { _, _ in
                withAnimation(.easeOut(duration: 0.15)) {
                    proxy.scrollTo("log-bottom", anchor: .bottom)
                }
            }
        }
    }

    private static let orderedServerTables = [
        "health_records",
        "workouts",
        "activity_summaries",
        "profile_snapshots",
        "electrocardiograms",
        "workout_routes",
        "heartbeat_series",
        "audiograms",
        "state_of_mind_records",
        "correlations",
    ]

    private static func displayName(forServerTable tableName: String) -> String {
        switch tableName {
        case "health_records":
            return "Records"
        case "workouts":
            return "Workouts"
        case "activity_summaries":
            return "Activity Summaries"
        case "profile_snapshots":
            return "Profile Snapshots"
        case "electrocardiograms":
            return "ECGs"
        case "workout_routes":
            return "Workout Routes"
        case "heartbeat_series":
            return "Heartbeat Series"
        case "audiograms":
            return "Audiograms"
        case "state_of_mind_records":
            return "State of Mind"
        case "correlations":
            return "Correlations"
        default:
            return tableName
        }
    }

    private static func formatTimestamp(_ date: Date) -> String {
        date.formatted(date: .abbreviated, time: .standard)
    }

    private static func formatTimestamp(_ isoString: String?) -> String? {
        guard let isoString else { return nil }
        if let date = Self.parseISO8601Date(isoString) {
            return formatTimestamp(date)
        }
        return isoString
    }

    private static func parseISO8601Date(_ isoString: String) -> Date? {
        if let date = fractionalISO8601Formatter.date(from: isoString) {
            return date
        }
        return plainISO8601Formatter.date(from: isoString)
    }

    private static let fractionalISO8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let plainISO8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()
}
