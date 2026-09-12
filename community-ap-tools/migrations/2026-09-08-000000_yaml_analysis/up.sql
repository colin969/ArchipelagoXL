CREATE TABLE yaml_analysis_status (
    room_id         UUID        NOT NULL,
    yaml_id         UUID        NOT NULL,
    status          TEXT        NOT NULL,
    success         INTEGER     NOT NULL DEFAULT 0,
    total_checks    INTEGER     NOT NULL DEFAULT 0,
    starting_checks INTEGER     NOT NULL DEFAULT 0,
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, yaml_id)
);