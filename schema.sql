CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =================================================================
-- I. DEFINIÇÃO DOS TIPOS ENUMERADOS (ENUMS)
-- =================================================================

CREATE TYPE "SensorType" AS ENUM ('TEMPERATURE', 'HUMIDITY', 'SOIL_MOISTURE', 'LUMINOSITY');
CREATE TYPE "ActuatorType" AS ENUM ('WATER_PUMP', 'FAN', 'LIGHT');
CREATE TYPE "ActuatorControlMode" AS ENUM ('MANUAL', 'AUTOMATIC');

-- =================================================================
-- II. CRIAÇÃO DAS TABELAS
-- =================================================================

CREATE TABLE "users" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "email" VARCHAR(255) UNIQUE NOT NULL,
    "password_hash" VARCHAR(255) NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE "environments" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "description" TEXT,
    "apiKey" UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    "user_id" UUID NOT NULL REFERENCES "users"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE "sensors" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "type" "SensorType" NOT NULL,
    "environment_id" UUID NOT NULL REFERENCES "environments"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE "actuators" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "type" "ActuatorType" NOT NULL,
    "is_on" BOOLEAN NOT NULL DEFAULT false,
    "control_mode" "ActuatorControlMode" NOT NULL DEFAULT 'MANUAL',
    "environment_id" UUID NOT NULL REFERENCES "environments"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Leituras dos Sensores
CREATE TABLE "sensor_readings" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "value" DOUBLE PRECISION NOT NULL,
    "sensor_id" UUID NOT NULL REFERENCES "sensors"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE "actuator_logs" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "action" VARCHAR(50) NOT NULL,
    "triggered_by" VARCHAR(50) NOT NULL,
    "actuator_id" UUID NOT NULL REFERENCES "actuators"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE "rules" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "trigger_condition" VARCHAR(50) NOT NULL, 
    "trigger_value" DOUBLE PRECISION NOT NULL,
    "action_type" VARCHAR(50) NOT NULL, 
    "environment_id" UUID NOT NULL REFERENCES "environments"("id") ON DELETE CASCADE,
    "trigger_sensor_id" UUID NOT NULL REFERENCES "sensors"("id") ON DELETE CASCADE,
    "action_actuator_id" UUID NOT NULL REFERENCES "actuators"("id") ON DELETE CASCADE,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =================================================================
-- III. FUNÇÃO E TRIGGERS PARA ATUALIZAÇÃO AUTOMÁTICA
-- =================================================================


CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';



CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON "users" FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_environments_updated_at BEFORE UPDATE ON "environments" FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_sensors_updated_at BEFORE UPDATE ON "sensors" FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_actuators_updated_at BEFORE UPDATE ON "actuators" FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
CREATE TRIGGER update_rules_updated_at BEFORE UPDATE ON "rules" FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
-- ... (no final do arquivo, depois de todos os CREATE TRIGGER)

-- =================================================================
-- IV. ADIÇÕES PARA VERSIONAMENTO DE REGRAS (FASE 4)
-- =================================================================

-- Adiciona colunas na tabela 'rules' para o versionamento
ALTER TABLE "rules" ADD COLUMN "version" INTEGER NOT NULL DEFAULT 1;
ALTER TABLE "rules" ADD COLUMN "is_active" BOOLEAN NOT NULL DEFAULT true;
-- `rule_group_id` vai agrupar todas as versões de uma mesma regra. A primeira versão aponta para si mesma.
ALTER TABLE "rules" ADD COLUMN "rule_group_id" UUID;
UPDATE "rules" SET "rule_group_id" = "id"; -- Define o valor inicial para as regras existentes
ALTER TABLE "rules" ALTER COLUMN "rule_group_id" SET NOT NULL; -- Torna a coluna obrigatória