import HealthKit
import Foundation


// MARK: - Data Models (mirror server's Pydantic models)

struct HealthRecord: Codable {
    let sampleUUID: String
    let recordType: String
    let value: Double?
    let unit: String?
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case recordType = "record_type"
        case value, unit
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case metadata
    }
}

struct WorkoutRecord: Codable {
    let sampleUUID: String
    let workoutType: String
    let startDate: String
    let endDate: String?
    let durationSeconds: Double?
    let totalEnergyBurned: Double?
    let totalDistance: Double?
    let sourceName: String?
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case workoutType = "workout_type"
        case startDate = "start_date"
        case endDate = "end_date"
        case durationSeconds = "duration_seconds"
        case totalEnergyBurned = "total_energy_burned"
        case totalDistance = "total_distance"
        case sourceName = "source_name"
        case metadata
    }
}

struct ElectrocardiogramVoltageMeasurementRecord: Codable {
    let timeSinceSampleStart: Double
    let leadValues: [String: Double]

    enum CodingKeys: String, CodingKey {
        case timeSinceSampleStart = "time_since_sample_start"
        case leadValues = "lead_values"
    }
}

struct ElectrocardiogramRecord: Codable {
    let sampleUUID: String
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let numberOfVoltageMeasurements: Int
    let samplingFrequencyHz: Double?
    let classificationCode: Int
    let symptomsStatusCode: Int
    let averageHeartRateBpm: Double?
    let voltageMeasurements: [ElectrocardiogramVoltageMeasurementRecord]
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case numberOfVoltageMeasurements = "number_of_voltage_measurements"
        case samplingFrequencyHz = "sampling_frequency_hz"
        case classificationCode = "classification_code"
        case symptomsStatusCode = "symptoms_status_code"
        case averageHeartRateBpm = "average_heart_rate_bpm"
        case voltageMeasurements = "voltage_measurements"
        case metadata
    }
}

struct WorkoutRouteLocationRecord: Codable {
    let timestamp: String
    let latitude: Double
    let longitude: Double
    let altitude: Double
    let horizontalAccuracy: Double
    let verticalAccuracy: Double
    let course: Double
    let speed: Double

    enum CodingKeys: String, CodingKey {
        case timestamp, latitude, longitude, altitude, course, speed
        case horizontalAccuracy = "horizontal_accuracy"
        case verticalAccuracy = "vertical_accuracy"
    }
}

struct WorkoutRouteRecord: Codable {
    let sampleUUID: String
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let locations: [WorkoutRouteLocationRecord]
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case locations
        case metadata
    }
}

struct HeartbeatSeriesBeatRecord: Codable {
    let timeSinceSeriesStart: Double
    let precededByGap: Bool

    enum CodingKeys: String, CodingKey {
        case timeSinceSeriesStart = "time_since_series_start"
        case precededByGap = "preceded_by_gap"
    }
}

struct HeartbeatSeriesRecord: Codable {
    let sampleUUID: String
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let beats: [HeartbeatSeriesBeatRecord]
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case beats
        case metadata
    }
}

struct AudiogramSensitivityTestRecord: Codable {
    let sensitivityDbHL: Double
    let conductionTypeCode: Int
    let masked: Bool
    let sideCode: Int
    let clampingLowerBoundDbHL: Double?
    let clampingUpperBoundDbHL: Double?

    enum CodingKeys: String, CodingKey {
        case sensitivityDbHL = "sensitivity_dbhl"
        case conductionTypeCode = "conduction_type_code"
        case masked
        case sideCode = "side_code"
        case clampingLowerBoundDbHL = "clamping_lower_bound_dbhl"
        case clampingUpperBoundDbHL = "clamping_upper_bound_dbhl"
    }
}

struct AudiogramSensitivityPointRecord: Codable {
    let frequencyHz: Double
    let leftEarSensitivityDbHL: Double?
    let rightEarSensitivityDbHL: Double?
    let tests: [AudiogramSensitivityTestRecord]

    enum CodingKeys: String, CodingKey {
        case frequencyHz = "frequency_hz"
        case leftEarSensitivityDbHL = "left_ear_sensitivity_dbhl"
        case rightEarSensitivityDbHL = "right_ear_sensitivity_dbhl"
        case tests
    }
}

struct AudiogramRecord: Codable {
    let sampleUUID: String
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let sensitivityPoints: [AudiogramSensitivityPointRecord]
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case sensitivityPoints = "sensitivity_points"
        case metadata
    }
}

struct StateOfMindRecord: Codable {
    let sampleUUID: String
    let startDate: String
    let endDate: String?
    let device: String?
    let sourceName: String?
    let kindCode: Int
    let valence: Double
    let valenceClassificationCode: Int
    let labelCodes: [Int]
    let associationCodes: [Int]
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case sampleUUID = "sample_uuid"
        case startDate = "start_date"
        case endDate = "end_date"
        case device
        case sourceName = "source_name"
        case kindCode = "kind_code"
        case valence
        case valenceClassificationCode = "valence_classification_code"
        case labelCodes = "label_codes"
        case associationCodes = "association_codes"
        case metadata
    }
}

struct BatchPayload: Codable {
    var records: [HealthRecord] = []
    var workouts: [WorkoutRecord] = []
    var electrocardiograms: [ElectrocardiogramRecord] = []
    var workoutRoutes: [WorkoutRouteRecord] = []
    var heartbeatSeries: [HeartbeatSeriesRecord] = []
    var audiograms: [AudiogramRecord] = []
    var stateOfMind: [StateOfMindRecord] = []

    enum CodingKeys: String, CodingKey {
        case records, workouts
        case electrocardiograms
        case workoutRoutes = "workout_routes"
        case heartbeatSeries = "heartbeat_series"
        case audiograms
        case stateOfMind = "state_of_mind"
    }

    var isEmpty: Bool {
        records.isEmpty &&
        workouts.isEmpty &&
        electrocardiograms.isEmpty &&
        workoutRoutes.isEmpty &&
        heartbeatSeries.isEmpty &&
        audiograms.isEmpty &&
        stateOfMind.isEmpty
    }

    mutating func merge(_ other: BatchPayload) {
        records += other.records
        workouts += other.workouts
        electrocardiograms += other.electrocardiograms
        workoutRoutes += other.workoutRoutes
        heartbeatSeries += other.heartbeatSeries
        audiograms += other.audiograms
        stateOfMind += other.stateOfMind
    }

    var sampleCount: Int {
        records.count +
        workouts.count +
        electrocardiograms.count +
        workoutRoutes.count +
        heartbeatSeries.count +
        audiograms.count +
        stateOfMind.count
    }
}

// MARK: - HealthKit type lists and serialization (stateless)

enum HKSync {

    static let isoFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()

    // MARK: All quantity types we want to track

    static var allQuantityTypes: [(HKQuantityType, HKUnit)] {
        var types: [(HKQuantityType, HKUnit)] = []

        let add: (HKQuantityTypeIdentifier, HKUnit) -> Void = { id, unit in
            if let t = HKQuantityType.quantityType(forIdentifier: id) {
                types.append((t, unit))
            }
        }

        // Activity
        add(.stepCount, .count())
        add(.distanceWalkingRunning, .meter())
        add(.distanceCycling, .meter())
        add(.distanceSwimming, .meter())
        add(.distanceDownhillSnowSports, .meter())
        add(.distanceWheelchair, .meter())
        add(.activeEnergyBurned, .kilocalorie())
        add(.basalEnergyBurned, .kilocalorie())
        add(.appleMoveTime, .minute())
        add(.flightsClimbed, .count())
        add(.appleExerciseTime, .minute())
        add(.appleStandTime, .minute())
        add(.pushCount, .count())
        add(.swimmingStrokeCount, .count())
        add(.nikeFuel, .count())
        add(.appleWalkingSteadiness, .percent())
        add(.vo2Max, HKUnit(from: "mL/(kg*min)"))

        if #available(iOS 16.0, *) {
            add(.appleSleepingWristTemperature, .degreeCelsius())
            add(.environmentalSoundReduction, .decibelAWeightedSoundPressureLevel())
            add(.atrialFibrillationBurden, .percent())
            add(.heartRateRecoveryOneMinute, HKUnit(from: "count/min"))
            add(.underwaterDepth, .meter())
            add(.waterTemperature, .degreeCelsius())
        }

        if #available(iOS 17.0, *) {
            add(.cyclingCadence, HKUnit(from: "count/min"))
            add(.cyclingFunctionalThresholdPower, .watt())
            add(.cyclingPower, .watt())
            add(.cyclingSpeed, HKUnit(from: "m/s"))
            add(.physicalEffort, HKUnit(from: "kcal/(kg*hr)"))
            add(.timeInDaylight, .minute())
        }

        if #available(iOS 18.0, *) {
            add(.crossCountrySkiingSpeed, HKUnit(from: "m/s"))
            add(.distanceCrossCountrySkiing, .meter())
            add(.distancePaddleSports, .meter())
            add(.distanceRowing, .meter())
            add(.distanceSkatingSports, .meter())
            add(.estimatedWorkoutEffortScore, .appleEffortScore())
            add(.paddleSportsSpeed, HKUnit(from: "m/s"))
            add(.rowingSpeed, HKUnit(from: "m/s"))
            add(.workoutEffortScore, .appleEffortScore())
            add(.appleSleepingBreathingDisturbances, .count())
        }

        // Heart
        add(.heartRate, HKUnit(from: "count/min"))
        add(.restingHeartRate, HKUnit(from: "count/min"))
        add(.walkingHeartRateAverage, HKUnit(from: "count/min"))
        add(.heartRateVariabilitySDNN, .secondUnit(with: .milli))
        add(.peripheralPerfusionIndex, .percent())

        // Body measurements
        add(.height, .meter())
        add(.bodyMass, .gramUnit(with: .kilo))
        add(.bodyMassIndex, .count())
        add(.bodyFatPercentage, .percent())
        add(.leanBodyMass, .gramUnit(with: .kilo))
        add(.waistCircumference, .meter())

        // Vitals
        add(.oxygenSaturation, .percent())
        add(.bodyTemperature, .degreeCelsius())
        add(.basalBodyTemperature, .degreeCelsius())
        add(.bloodPressureSystolic, .millimeterOfMercury())
        add(.bloodPressureDiastolic, .millimeterOfMercury())
        add(.respiratoryRate, HKUnit(from: "count/min"))

        // Metabolic
        add(.bloodGlucose, HKUnit(from: "mg/dL"))
        add(.bloodAlcoholContent, .percent())
        add(.dietaryEnergyConsumed, .kilocalorie())
        add(.dietaryBiotin, .gramUnit(with: .micro))
        add(.dietaryCarbohydrates, .gramUnit(with: .none))
        add(.dietaryChloride, .gramUnit(with: .milli))
        add(.dietaryCholesterol, .gramUnit(with: .milli))
        add(.dietaryChromium, .gramUnit(with: .micro))
        add(.dietaryCopper, .gramUnit(with: .milli))
        add(.dietaryFiber, .gramUnit(with: .none))
        add(.dietaryFatMonounsaturated, .gramUnit(with: .none))
        add(.dietaryFatPolyunsaturated, .gramUnit(with: .none))
        add(.dietarySugar, .gramUnit(with: .none))
        add(.dietaryFatTotal, .gramUnit(with: .none))
        add(.dietaryFatSaturated, .gramUnit(with: .none))
        add(.dietaryFolate, .gramUnit(with: .micro))
        add(.dietaryIodine, .gramUnit(with: .micro))
        add(.dietaryProtein, .gramUnit(with: .none))
        add(.dietarySodium, .gramUnit(with: .milli))
        add(.dietaryPotassium, .gramUnit(with: .milli))
        add(.dietaryCalcium, .gramUnit(with: .milli))
        add(.dietaryIron, .gramUnit(with: .milli))
        add(.dietaryMagnesium, .gramUnit(with: .milli))
        add(.dietaryManganese, .gramUnit(with: .milli))
        add(.dietaryMolybdenum, .gramUnit(with: .micro))
        add(.dietaryNiacin, .gramUnit(with: .milli))
        add(.dietaryPantothenicAcid, .gramUnit(with: .milli))
        add(.dietaryPhosphorus, .gramUnit(with: .milli))
        add(.dietaryRiboflavin, .gramUnit(with: .milli))
        add(.dietarySelenium, .gramUnit(with: .micro))
        add(.dietaryThiamin, .gramUnit(with: .milli))
        add(.dietaryVitaminA, .gramUnit(with: .micro))
        add(.dietaryVitaminB12, .gramUnit(with: .micro))
        add(.dietaryVitaminB6, .gramUnit(with: .milli))
        add(.dietaryVitaminC, .gramUnit(with: .milli))
        add(.dietaryVitaminD, .gramUnit(with: .micro))
        add(.dietaryVitaminE, .gramUnit(with: .milli))
        add(.dietaryVitaminK, .gramUnit(with: .micro))
        add(.dietaryZinc, .gramUnit(with: .milli))
        add(.dietaryWater, .liter())
        add(.dietaryCaffeine, .gramUnit(with: .milli))

        // Hearing
        add(.environmentalAudioExposure, .decibelAWeightedSoundPressureLevel())
        add(.headphoneAudioExposure, .decibelAWeightedSoundPressureLevel())

        // Mobility
        add(.walkingSpeed, HKUnit(from: "m/s"))
        add(.walkingStepLength, .meter())
        add(.walkingAsymmetryPercentage, .percent())
        add(.walkingDoubleSupportPercentage, .percent())
        add(.stairAscentSpeed, HKUnit(from: "m/s"))
        add(.stairDescentSpeed, HKUnit(from: "m/s"))
        add(.sixMinuteWalkTestDistance, .meter())
        add(.runningSpeed, HKUnit(from: "m/s"))
        add(.runningPower, .watt())
        add(.runningVerticalOscillation, .meter())
        add(.runningGroundContactTime, .secondUnit(with: .milli))
        add(.runningStrideLength, .meter())

        // Other
        add(.uvExposure, .count())
        add(.numberOfTimesFallen, .count())
        add(.numberOfAlcoholicBeverages, .count())
        add(.electrodermalActivity, HKUnit(from: "mcS"))
        add(.insulinDelivery, HKUnit(from: "IU"))
        add(.inhalerUsage, .count())
        add(.forcedVitalCapacity, .liter())
        add(.forcedExpiratoryVolume1, .liter())
        add(.peakExpiratoryFlowRate, HKUnit(from: "L/min"))

        return types
    }

    static var allCategoryTypes: [HKCategoryType] {
        var ids: [HKCategoryTypeIdentifier] = [
            .sleepAnalysis,
            .appleStandHour,
            .menstrualFlow,
            .cervicalMucusQuality,
            .ovulationTestResult,
            .contraceptive,
            .infrequentMenstrualCycles,
            .pregnancy,
            .lactation,
            .intermenstrualBleeding,
            .persistentIntermenstrualBleeding,
            .prolongedMenstrualPeriods,
            .irregularMenstrualCycles,
            .pregnancyTestResult,
            .progesteroneTestResult,
            .sexualActivity,
            .mindfulSession,
            .highHeartRateEvent,
            .lowHeartRateEvent,
            .irregularHeartRhythmEvent,
            .lowCardioFitnessEvent,
            .toothbrushingEvent,
            .handwashingEvent,
            .headphoneAudioExposureEvent,
            .environmentalAudioExposureEvent,
            .appleWalkingSteadinessEvent,
            .abdominalCramps,
            .acne,
            .appetiteChanges,
            .bladderIncontinence,
            .bloating,
            .breastPain,
            .chestTightnessOrPain,
            .chills,
            .constipation,
            .coughing,
            .diarrhea,
            .dizziness,
            .drySkin,
            .fainting,
            .fatigue,
            .fever,
            .generalizedBodyAche,
            .hairLoss,
            .headache,
            .heartburn,
            .hotFlashes,
            .lossOfSmell,
            .lossOfTaste,
            .lowerBackPain,
            .memoryLapse,
            .moodChanges,
            .nausea,
            .nightSweats,
            .pelvicPain,
            .rapidPoundingOrFlutteringHeartbeat,
            .runnyNose,
            .shortnessOfBreath,
            .sinusCongestion,
            .skippedHeartbeat,
            .sleepChanges,
            .soreThroat,
            .vaginalDryness,
            .vomiting,
            .wheezing,
        ]

        if #available(iOS 18.0, *) {
            ids.append(.bleedingAfterPregnancy)
            ids.append(.bleedingDuringPregnancy)
            ids.append(.sleepApneaEvent)
        }

        return ids.compactMap { HKCategoryType.categoryType(forIdentifier: $0) }
    }

    static var allAdditionalSampleTypes: [HKSampleType] {
        [
            HKWorkoutType.workoutType(),
            HKSeriesType.workoutRoute(),
            HKSeriesType.heartbeat(),
            HKObjectType.audiogramSampleType(),
            HKObjectType.electrocardiogramType(),
            HKObjectType.stateOfMindType(),
        ]
    }

    static var allCharacteristicTypes: [HKCharacteristicType] {
        let ids: [HKCharacteristicTypeIdentifier] = [
            .dateOfBirth,
            .biologicalSex,
            .bloodType,
            .fitzpatrickSkinType,
            .wheelchairUse,
            .activityMoveMode,
        ]

        return ids.compactMap { HKCharacteristicType.characteristicType(forIdentifier: $0) }
    }

    static var allSampleTypes: [HKSampleType] {
        allQuantityTypes.map(\.0) +
        allCategoryTypes +
        allAdditionalSampleTypes
    }

    // MARK: All read types for permissions request

    static var allReadTypes: Set<HKObjectType> {
        var types = Set<HKObjectType>()
        for sampleType in allSampleTypes {
            types.insert(sampleType)
        }
        for characteristicType in allCharacteristicTypes { types.insert(characteristicType) }
        return types
    }

    // MARK: - Profile characteristics

    struct ProfileCharacteristics {
        var dateOfBirth: String?
        var biologicalSexCode: Int?
        var bloodTypeCode: Int?
        var fitzpatrickSkinTypeCode: Int?
        var wheelchairUseCode: Int?
        var activityMoveModeCode: Int?
    }

    static func readCharacteristics(from store: HKHealthStore) -> ProfileCharacteristics {
        var profile = ProfileCharacteristics()
        if let dob = try? store.dateOfBirthComponents().date {
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd"
            profile.dateOfBirth = formatter.string(from: dob)
        }
        if let sex = try? store.biologicalSex().biologicalSex, sex != .notSet {
            profile.biologicalSexCode = sex.rawValue
        }
        if let blood = try? store.bloodType().bloodType, blood != .notSet {
            profile.bloodTypeCode = blood.rawValue
        }
        if let skin = try? store.fitzpatrickSkinType().skinType, skin != .notSet {
            profile.fitzpatrickSkinTypeCode = skin.rawValue
        }
        if let wheelchair = try? store.wheelchairUse().wheelchairUse, wheelchair != .notSet {
            profile.wheelchairUseCode = wheelchair.rawValue
        }
        if let moveMode = try? store.activityMoveMode().activityMoveMode {
            profile.activityMoveModeCode = moveMode.rawValue
        }
        return profile
    }

    // MARK: - Serialization

    static func serialize(_ sample: HKSample, store: HKHealthStore) async -> BatchPayload? {
        let device = sample.device?.name
        let source = sample.sourceRevision.source.name
        let metadata = metadataStrings(from: sample.metadata)
        var payload = BatchPayload()

        if let qty = sample as? HKQuantitySample {
            let unit = Self.preferredUnit(for: qty.quantityType)
            let value = qty.quantity.doubleValue(for: unit)
            payload.records.append(HealthRecord(
                sampleUUID: qty.uuid.uuidString,
                recordType: qty.quantityType.identifier,
                value: value,
                unit: unit.unitString,
                startDate: isoFormatter.string(from: qty.startDate),
                endDate: isoFormatter.string(from: qty.endDate),
                device: device,
                sourceName: source,
                metadata: metadata
            ))
            return payload
        }

        if let cat = sample as? HKCategorySample {
            payload.records.append(HealthRecord(
                sampleUUID: cat.uuid.uuidString,
                recordType: cat.categoryType.identifier,
                value: Double(cat.value),
                unit: nil,
                startDate: isoFormatter.string(from: cat.startDate),
                endDate: isoFormatter.string(from: cat.endDate),
                device: device,
                sourceName: source,
                metadata: metadata
            ))
            return payload
        }

        if let workout = sample as? HKWorkout {
            payload.workouts.append(WorkoutRecord(
                sampleUUID: workout.uuid.uuidString,
                workoutType: workout.workoutActivityType.name,
                startDate: isoFormatter.string(from: workout.startDate),
                endDate: isoFormatter.string(from: workout.endDate),
                durationSeconds: workout.duration,
                totalEnergyBurned: workoutQuantitySum(for: .activeEnergyBurned, in: workout, unit: .kilocalorie()),
                totalDistance: workoutTotalDistance(for: workout),
                sourceName: source,
                metadata: metadata
            ))
            return payload
        }

        if let electrocardiogram = sample as? HKElectrocardiogram {
            guard let voltageMeasurements = await Self.fetchElectrocardiogramMeasurements(for: electrocardiogram, store: store) else {
                return nil
            }
            payload.electrocardiograms.append(ElectrocardiogramRecord(
                sampleUUID: electrocardiogram.uuid.uuidString,
                startDate: isoFormatter.string(from: electrocardiogram.startDate),
                endDate: isoFormatter.string(from: electrocardiogram.endDate),
                device: device,
                sourceName: source,
                numberOfVoltageMeasurements: electrocardiogram.numberOfVoltageMeasurements,
                samplingFrequencyHz: electrocardiogram.samplingFrequency?.doubleValue(for: .hertz()),
                classificationCode: electrocardiogram.classification.rawValue,
                symptomsStatusCode: electrocardiogram.symptomsStatus.rawValue,
                averageHeartRateBpm: electrocardiogram.averageHeartRate?.doubleValue(for: HKUnit(from: "count/min")),
                voltageMeasurements: voltageMeasurements,
                metadata: metadata
            ))
            return payload
        }

        if let workoutRoute = sample as? HKWorkoutRoute {
            guard let locations = await Self.fetchWorkoutRouteLocations(for: workoutRoute, store: store) else {
                return nil
            }
            payload.workoutRoutes.append(WorkoutRouteRecord(
                sampleUUID: workoutRoute.uuid.uuidString,
                startDate: isoFormatter.string(from: workoutRoute.startDate),
                endDate: isoFormatter.string(from: workoutRoute.endDate),
                device: device,
                sourceName: source,
                locations: locations,
                metadata: metadata
            ))
            return payload
        }

        if let heartbeatSeries = sample as? HKHeartbeatSeriesSample {
            guard let beats = await Self.fetchHeartbeatSeriesBeats(for: heartbeatSeries, store: store) else {
                return nil
            }
            payload.heartbeatSeries.append(HeartbeatSeriesRecord(
                sampleUUID: heartbeatSeries.uuid.uuidString,
                startDate: isoFormatter.string(from: heartbeatSeries.startDate),
                endDate: isoFormatter.string(from: heartbeatSeries.endDate),
                device: device,
                sourceName: source,
                beats: beats,
                metadata: metadata
            ))
            return payload
        }

        if let audiogram = sample as? HKAudiogramSample {
            payload.audiograms.append(AudiogramRecord(
                sampleUUID: audiogram.uuid.uuidString,
                startDate: isoFormatter.string(from: audiogram.startDate),
                endDate: isoFormatter.string(from: audiogram.endDate),
                device: device,
                sourceName: source,
                sensitivityPoints: serializeSensitivityPoints(from: audiogram),
                metadata: metadata
            ))
            return payload
        }

        if #available(iOS 18.0, *), let stateOfMind = sample as? HKStateOfMind {
            payload.stateOfMind.append(StateOfMindRecord(
                sampleUUID: stateOfMind.uuid.uuidString,
                startDate: isoFormatter.string(from: stateOfMind.startDate),
                endDate: isoFormatter.string(from: stateOfMind.endDate),
                device: device,
                sourceName: source,
                kindCode: stateOfMind.kind.rawValue,
                valence: stateOfMind.valence,
                valenceClassificationCode: stateOfMind.valenceClassification.rawValue,
                labelCodes: stateOfMind.labels.map(\.rawValue),
                associationCodes: stateOfMind.associations.map(\.rawValue),
                metadata: metadata
            ))
            return payload
        }

        return nil
    }

    private static func fetchElectrocardiogramMeasurements(
        for electrocardiogram: HKElectrocardiogram,
        store: HKHealthStore
    ) async -> [ElectrocardiogramVoltageMeasurementRecord]? {
        let healthStore = store

        return await withCheckedContinuation { continuation in
            var records: [ElectrocardiogramVoltageMeasurementRecord] = []
            var didResume = false

            let query = HKElectrocardiogramQuery(electrocardiogram: electrocardiogram) { query, measurement, done, error in
                guard !didResume else { return }

                if let error {
                    didResume = true
                    healthStore.stop(query)
                    print("[HealthKitManager] ECG voltage query failed: \(error.localizedDescription)")
                    continuation.resume(returning: nil)
                    return
                }

                if let measurement,
                   let voltage = measurement.quantity(for: .appleWatchSimilarToLeadI) {
                    records.append(ElectrocardiogramVoltageMeasurementRecord(
                        timeSinceSampleStart: measurement.timeSinceSampleStart,
                        leadValues: [
                            "apple_watch_similar_to_lead_i": voltage.doubleValue(for: .volt())
                        ]
                    ))
                }

                if done {
                    didResume = true
                    continuation.resume(returning: records)
                }
            }

            healthStore.execute(query)
        }
    }

    private static func fetchWorkoutRouteLocations(
        for workoutRoute: HKWorkoutRoute,
        store: HKHealthStore
    ) async -> [WorkoutRouteLocationRecord]? {
        let healthStore = store

        return await withCheckedContinuation { continuation in
            var records: [WorkoutRouteLocationRecord] = []
            var didResume = false

            let query = HKWorkoutRouteQuery(route: workoutRoute) { query, locations, done, error in
                guard !didResume else { return }

                if let error {
                    didResume = true
                    healthStore.stop(query)
                    print("[HealthKitManager] Workout route query failed: \(error.localizedDescription)")
                    continuation.resume(returning: nil)
                    return
                }

                for location in locations ?? [] {
                    records.append(WorkoutRouteLocationRecord(
                        timestamp: Self.iso8601Timestamp(for: location.timestamp),
                        latitude: location.coordinate.latitude,
                        longitude: location.coordinate.longitude,
                        altitude: location.altitude,
                        horizontalAccuracy: location.horizontalAccuracy,
                        verticalAccuracy: location.verticalAccuracy,
                        course: location.course,
                        speed: location.speed
                    ))
                }

                if done {
                    didResume = true
                    continuation.resume(returning: records)
                }
            }

            healthStore.execute(query)
        }
    }

    private static func fetchHeartbeatSeriesBeats(
        for heartbeatSeries: HKHeartbeatSeriesSample,
        store: HKHealthStore
    ) async -> [HeartbeatSeriesBeatRecord]? {
        let healthStore = store

        return await withCheckedContinuation { continuation in
            var records: [HeartbeatSeriesBeatRecord] = []
            var didResume = false

            let query = HKHeartbeatSeriesQuery(heartbeatSeries: heartbeatSeries) { query, timeSinceSeriesStart, precededByGap, done, error in
                guard !didResume else { return }

                if let error {
                    didResume = true
                    healthStore.stop(query)
                    print("[HealthKitManager] Heartbeat series query failed: \(error.localizedDescription)")
                    continuation.resume(returning: nil)
                    return
                }

                if !done {
                    records.append(HeartbeatSeriesBeatRecord(
                        timeSinceSeriesStart: timeSinceSeriesStart,
                        precededByGap: precededByGap
                    ))
                }

                if done {
                    didResume = true
                    continuation.resume(returning: records)
                }
            }

            healthStore.execute(query)
        }
    }

    private static func serializeSensitivityPoints(from audiogram: HKAudiogramSample) -> [AudiogramSensitivityPointRecord] {
        audiogram.sensitivityPoints.map { point in
            let tests: [AudiogramSensitivityTestRecord]
            let leftEarSensitivity: Double?
            let rightEarSensitivity: Double?

            if #available(iOS 18.1, *) {
                tests = point.tests.map { test in
                    AudiogramSensitivityTestRecord(
                        sensitivityDbHL: test.sensitivity.doubleValue(for: .decibelHearingLevel()),
                        conductionTypeCode: test.type.rawValue,
                        masked: test.masked,
                        sideCode: test.side.rawValue,
                        clampingLowerBoundDbHL: test.clampingRange?.lowerBound?.doubleValue(for: .decibelHearingLevel()),
                        clampingUpperBoundDbHL: test.clampingRange?.upperBound?.doubleValue(for: .decibelHearingLevel())
                    )
                }
                leftEarSensitivity = tests.first(where: { $0.sideCode == HKAudiogramSensitivityTestSide.left.rawValue })?.sensitivityDbHL
                rightEarSensitivity = tests.first(where: { $0.sideCode == HKAudiogramSensitivityTestSide.right.rawValue })?.sensitivityDbHL
            } else {
                tests = []
                leftEarSensitivity = point.leftEarSensitivity?.doubleValue(for: .decibelHearingLevel())
                rightEarSensitivity = point.rightEarSensitivity?.doubleValue(for: .decibelHearingLevel())
            }

            return AudiogramSensitivityPointRecord(
                frequencyHz: point.frequency.doubleValue(for: .hertz()),
                leftEarSensitivityDbHL: leftEarSensitivity,
                rightEarSensitivityDbHL: rightEarSensitivity,
                tests: tests
            )
        }
    }

    private static func metadataStrings(from metadata: [String: Any]?) -> [String: String]? {
        guard let metadata, !metadata.isEmpty else { return nil }

        var serialized: [String: String] = [:]
        for (key, value) in metadata {
            if let date = value as? Date {
                serialized[key] = isoFormatter.string(from: date)
            } else {
                serialized[key] = String(describing: value)
            }
        }
        return serialized.isEmpty ? nil : serialized
    }

    private static func workoutQuantitySum(
        for identifier: HKQuantityTypeIdentifier,
        in workout: HKWorkout,
        unit: HKUnit
    ) -> Double? {
        guard let quantityType = HKQuantityType.quantityType(forIdentifier: identifier),
              let quantity = workout.statistics(for: quantityType)?.sumQuantity()
        else {
            return nil
        }

        return quantity.doubleValue(for: unit)
    }

    private static func workoutTotalDistance(for workout: HKWorkout) -> Double? {
        let candidateTypes: [HKQuantityTypeIdentifier] = [
            .distanceWalkingRunning,
            .distanceCycling,
            .distanceSwimming,
            .distanceWheelchair,
            .distanceDownhillSnowSports,
            .distanceCrossCountrySkiing,
            .distancePaddleSports,
            .distanceRowing,
            .distanceSkatingSports,
        ]

        for identifier in candidateTypes {
            if let value = workoutQuantitySum(for: identifier, in: workout, unit: .meter()) {
                return value
            }
        }

        return workout.totalDistance?.doubleValue(for: .meter())
    }

    private static func iso8601Timestamp(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }

    // MARK: - Unit helpers

    static func preferredUnit(for type: HKQuantityType) -> HKUnit {
        for (qt, unit) in allQuantityTypes {
            if qt == type { return unit }
        }
        return .count()
    }
}

// MARK: - Workout type name helper

extension HKWorkoutActivityType {
    var name: String {
        switch self {
        case .americanFootball: return "AmericanFootball"
        case .archery: return "Archery"
        case .australianFootball: return "AustralianFootball"
        case .badminton: return "Badminton"
        case .baseball: return "Baseball"
        case .basketball: return "Basketball"
        case .bowling: return "Bowling"
        case .boxing: return "Boxing"
        case .climbing: return "Climbing"
        case .cricket: return "Cricket"
        case .crossTraining: return "CrossTraining"
        case .curling: return "Curling"
        case .cycling: return "Cycling"
        case .dance: return "Dance"
        case .elliptical: return "Elliptical"
        case .equestrianSports: return "EquestrianSports"
        case .fencing: return "Fencing"
        case .fishing: return "Fishing"
        case .functionalStrengthTraining: return "FunctionalStrengthTraining"
        case .golf: return "Golf"
        case .gymnastics: return "Gymnastics"
        case .handball: return "Handball"
        case .hiking: return "Hiking"
        case .hockey: return "Hockey"
        case .hunting: return "Hunting"
        case .lacrosse: return "Lacrosse"
        case .martialArts: return "MartialArts"
        case .mindAndBody: return "MindAndBody"
        case .paddleSports: return "PaddleSports"
        case .play: return "Play"
        case .preparationAndRecovery: return "PreparationAndRecovery"
        case .racquetball: return "Racquetball"
        case .rowing: return "Rowing"
        case .rugby: return "Rugby"
        case .running: return "Running"
        case .sailing: return "Sailing"
        case .skatingSports: return "SkatingSports"
        case .snowSports: return "SnowSports"
        case .soccer: return "Soccer"
        case .softball: return "Softball"
        case .squash: return "Squash"
        case .stairClimbing: return "StairClimbing"
        case .surfingSports: return "SurfingSports"
        case .swimming: return "Swimming"
        case .tableTennis: return "TableTennis"
        case .tennis: return "Tennis"
        case .trackAndField: return "TrackAndField"
        case .traditionalStrengthTraining: return "TraditionalStrengthTraining"
        case .volleyball: return "Volleyball"
        case .walking: return "Walking"
        case .waterFitness: return "WaterFitness"
        case .waterPolo: return "WaterPolo"
        case .waterSports: return "WaterSports"
        case .wrestling: return "Wrestling"
        case .yoga: return "Yoga"
        case .highIntensityIntervalTraining: return "HIIT"
        case .jumpRope: return "JumpRope"
        case .kickboxing: return "Kickboxing"
        case .pilates: return "Pilates"
        case .snowboarding: return "Snowboarding"
        case .stairs: return "Stairs"
        case .stepTraining: return "StepTraining"
        case .wheelchairWalkPace: return "WheelchairWalkPace"
        case .wheelchairRunPace: return "WheelchairRunPace"
        case .taiChi: return "TaiChi"
        case .mixedCardio: return "MixedCardio"
        case .handCycling: return "HandCycling"
        case .discSports: return "DiscSports"
        case .fitnessGaming: return "FitnessGaming"
        case .cardioDance: return "CardioDance"
        case .socialDance: return "SocialDance"
        case .pickleball: return "Pickleball"
        case .cooldown: return "Cooldown"
        default: return "Other(\(rawValue))"
        }
    }
}
