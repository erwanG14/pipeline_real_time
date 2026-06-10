CREATE TABLE IF NOT EXISTS airplane_silver (
    hex VARCHAR(20),
    type VARCHAR(50),
    flight VARCHAR(50),
    r VARCHAR(50),
    t VARCHAR(50),
    "desc" TEXT,

    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    alt_geom INTEGER,
    alt_baro INTEGER,

    gs DOUBLE PRECISION,
    track DOUBLE PRECISION,
    geom_rate INTEGER,
    mach DOUBLE PRECISION,

    squawk VARCHAR(10),
    emergency VARCHAR(50),
    category VARCHAR(20),

    messages INTEGER,
    alert INTEGER,

    seen DOUBLE PRECISION,
    seen_pos DOUBLE PRECISION,

    timestamp_ingest TIMESTAMP,

    gs_km_h DOUBLE PRECISION,
    alt_baro_km DOUBLE PRECISION,
    alt_geom_km DOUBLE PRECISION,

    kpi_speed_type VARCHAR(50)
);