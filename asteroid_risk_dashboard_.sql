CREATE TABLE IF NOT EXISTS asteroid_risk_dashboard_raw (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    asteroid_full_name TEXT,
    hazardous_flag BOOLEAN,
    risk_score DOUBLE PRECISION,
    size_kpi TEXT,
    mach_kpi DOUBLE PRECISION,
    velocity_km_s_avg DOUBLE PRECISION,
    miss_distance_km_avg DOUBLE PRECISION,
    distance_lunar_equivalent DOUBLE PRECISION,
    is_close_approach BOOLEAN
);