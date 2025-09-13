-- ============================================================
-- RFID Zone Occupancy Database (Single File)
-- PostgreSQL 13+
-- ============================================================

CREATE SCHEMA IF NOT EXISTS rfid;
SET search_path = rfid, public;

-- =======================
-- Core reference tables
-- =======================

CREATE TABLE IF NOT EXISTS tag (
  tag_id        VARCHAR(128) PRIMARY KEY,
  registered_at TIMESTAMP WITH TIME ZONE,
  meta          JSONB
);

CREATE TABLE IF NOT EXISTS zone (
  zone_id       SERIAL PRIMARY KEY,
  zone_code     VARCHAR(32) UNIQUE NOT NULL,
  zone_type     VARCHAR(16) NOT NULL CHECK (zone_type IN ('REGISTER','EXIT','SUBZONE'))
);

CREATE TABLE IF NOT EXISTS reader (
  reader_id     SERIAL PRIMARY KEY,
  reader_code   VARCHAR(64) UNIQUE NOT NULL,
  zone_id       INTEGER NOT NULL UNIQUE REFERENCES zone(zone_id) ON DELETE RESTRICT
);

-- =======================
-- Person table (only tag_id)
-- =======================
CREATE TABLE IF NOT EXISTS person (
  person_id     SERIAL PRIMARY KEY,
  tag_id        VARCHAR(128) NOT NULL REFERENCES tag(tag_id)
);

-- =======================
-- RFID log table (provided)
-- =======================
CREATE TABLE IF NOT EXISTS rfid_log (
  id           SERIAL PRIMARY KEY,
  log_time     TIMESTAMP NOT NULL,
  rfid_card_id VARCHAR(128) NOT NULL,
  label        VARCHAR(50) NOT NULL
);

-- =======================
-- Movement facts
-- =======================
CREATE TABLE IF NOT EXISTS movement (
  move_id       BIGSERIAL PRIMARY KEY,
  tag_id        VARCHAR(128) NOT NULL REFERENCES tag(tag_id) ON DELETE CASCADE,
  from_zone_id  INTEGER REFERENCES zone(zone_id),
  to_zone_id    INTEGER NOT NULL REFERENCES zone(zone_id),
  move_time     TIMESTAMP WITH TIME ZONE NOT NULL,
  UNIQUE(tag_id, move_time, to_zone_id)
);

CREATE INDEX IF NOT EXISTS ix_movement_tag_time ON movement(tag_id, move_time);

-- =======================
-- Current occupancy snapshot
-- =======================
CREATE TABLE IF NOT EXISTS zone_occupancy (
  zone_id       INTEGER PRIMARY KEY REFERENCES zone(zone_id) ON DELETE CASCADE,
  as_of_time    TIMESTAMP WITH TIME ZONE NOT NULL,
  current_count INTEGER NOT NULL
);

-- =======================
-- Seed zones
-- =======================
INSERT INTO zone (zone_code, zone_type) VALUES
 ('REGISTER','REGISTER'),
 ('EXIT','EXIT'),
 ('Z1','SUBZONE'),
 ('Z2','SUBZONE'),
 ('Z3','SUBZONE'),
 ('Z4','SUBZONE'),
 ('Z5','SUBZONE'),
 ('Z6','SUBZONE'),
 ('Z7','SUBZONE'),
 ('Z8','SUBZONE')
ON CONFLICT (zone_code) DO NOTHING;

-- =======================
-- Derive movements from RFID logs
-- =======================
CREATE OR REPLACE FUNCTION refresh_and_load_movements() RETURNS VOID AS $$
DECLARE
  register_id INTEGER;
  exit_id INTEGER;
BEGIN
  SELECT zone_id INTO register_id FROM zone WHERE zone_code='REGISTER';
  SELECT zone_id INTO exit_id FROM zone WHERE zone_code='EXIT';

  -- Insert transitions between consecutive logs per tag
  WITH seq AS (
    SELECT
      rfid_card_id AS tag_id,
      label AS zone_code,
      log_time,
      LAG(label) OVER (PARTITION BY rfid_card_id ORDER BY log_time) AS prev_zone_code,
      LAG(log_time) OVER (PARTITION BY rfid_card_id ORDER BY log_time) AS prev_time
    FROM rfid_log
  ),
  zones AS (
    SELECT s.tag_id,
           z_prev.zone_id AS from_zone_id,
           z_to.zone_id AS to_zone_id,
           s.log_time AS move_time
    FROM seq s
    LEFT JOIN zone z_prev ON z_prev.zone_code = s.prev_zone_code
    JOIN zone z_to ON z_to.zone_code = s.zone_code
    WHERE s.prev_zone_code IS DISTINCT FROM s.zone_code OR s.prev_zone_code IS NULL
  )
  INSERT INTO movement(tag_id, from_zone_id, to_zone_id, move_time)
  SELECT tag_id, from_zone_id, to_zone_id, move_time
  FROM zones
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- =======================
-- Trigger to update occupancy
-- =======================
CREATE OR REPLACE FUNCTION apply_movement_deltas()
RETURNS TRIGGER AS $$
BEGIN
  -- increment destination
  INSERT INTO zone_occupancy(zone_id, as_of_time, current_count)
  VALUES (NEW.to_zone_id, NEW.move_time, 1)
  ON CONFLICT (zone_id) DO UPDATE
    SET current_count = zone_occupancy.current_count + 1,
        as_of_time = GREATEST(zone_occupancy.as_of_time, NEW.move_time);

  -- decrement source if exists
  IF NEW.from_zone_id IS NOT NULL THEN
    INSERT INTO zone_occupancy(zone_id, as_of_time, current_count)
    VALUES (NEW.from_zone_id, NEW.move_time, -1)
    ON CONFLICT (zone_id) DO UPDATE
      SET current_count = zone_occupancy.current_count - 1,
          as_of_time = GREATEST(zone_occupancy.as_of_time, NEW.move_time);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_movement_apply ON movement;
CREATE TRIGGER trg_movement_apply
AFTER INSERT ON movement
FOR EACH ROW EXECUTE FUNCTION apply_movement_deltas();

-- =======================
-- Current occupancy view
-- =======================
CREATE OR REPLACE VIEW v_zone_occupancy AS
SELECT z.zone_code, o.current_count, o.as_of_time
FROM zone_occupancy o
JOIN zone z ON z.zone_id=o.zone_id
WHERE z.zone_type='SUBZONE'
ORDER BY z.zone_code;
