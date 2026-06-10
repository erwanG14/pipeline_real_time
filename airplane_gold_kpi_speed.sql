CREATE TABLE IF NOT EXISTS airplane_gold_kpi_speed (
    window_start TIMESTAMP,
    window_end TIMESTAMP,

    kpi_speed_type VARCHAR(50),

    nb_plane_per_speed_type BIGINT,
    average_ground_speed_km_h_per_speed_type DOUBLE PRECISION,
    average_altitude_geom_km_per_speed_type DOUBLE PRECISION,
    nb_alert_per_speed_type BIGINT
);