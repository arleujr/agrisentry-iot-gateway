-- Up Migration

-- Enable pgcrypto for advanced cryptographic functions and UUID generation if required
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Enable TimescaleDB extension explicitly to handle time-series operations
CREATE EXTENSION IF NOT EXISTS "timescaledb" CASCADE;

-- 1. Create Data Quality Status ENUM to handle anti-redundancy pipeline stages
CREATE TYPE "DataQualityStatus" AS ENUM (
    'PENDING', 
    'VALID', 
    'ANOMALY_NOISE', 
    'ANOMALY_CRITICAL'
);

-- 2. Create standard relational table for Sensor Metadata (Static Asset Configuration)
CREATE TABLE "sensors" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "hardware_id" VARCHAR(100) UNIQUE NOT NULL, -- Hardware physical address (e.g., MAC or custom ESP32 string)
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT,
    "location_coordinates" POINT,               -- GIS/Precision Farming coordinates mapping
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create the Time-Series Hypertable for high-throughput raw telemetry ingestion
-- CRITICAL architectural note: The PRIMARY KEY MUST be composite and contain the partitioning time column ("created_at")
CREATE TABLE "sensor_readings" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "value" DOUBLE PRECISION NOT NULL,
    "sensor_id" UUID NOT NULL REFERENCES "sensors"("id") ON DELETE CASCADE,
    "status" "DataQualityStatus" NOT NULL DEFAULT 'PENDING',
    "ai_analysis_note" TEXT,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at)
);

-- 4. Convert the standard PostgreSQL table into a TimescaleDB Hypertable
-- Partitions data automatically in internal 'chunks' based on time intervals (default: 7 days per chunk)
SELECT create_hypertable('sensor_readings', 'created_at');

-- 5. Create specialized composite index for Machine Learning analytical lookups
-- Optimized for querying a specific sensor's timeline in descending order (newest data first)
CREATE INDEX "idx_sensor_readings_sensor_time" 
ON "sensor_readings" ("sensor_id", "created_at" DESC);


-- Down Migration (Rollback Strategy)
-- DROP TABLE IF EXISTS "sensor_readings" CASCADE;
-- DROP TABLE IF EXISTS "sensors" CASCADE;
-- DROP TYPE IF EXISTS "DataQualityStatus" CASCADE;