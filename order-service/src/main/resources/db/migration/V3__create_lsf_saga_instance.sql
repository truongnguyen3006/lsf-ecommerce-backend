CREATE TABLE IF NOT EXISTS lsf_saga_instance (
    saga_id VARCHAR(128) PRIMARY KEY,
    definition_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    phase VARCHAR(32) NOT NULL,
    current_step_index INTEGER,
    compensation_step_index INTEGER,
    current_step VARCHAR(128),
    correlation_id VARCHAR(128),
    request_id VARCHAR(128),
    causation_id VARCHAR(128),
    last_event_id VARCHAR(128),
    failure_reason VARCHAR(2000),
    state_json LONGTEXT NOT NULL,
    steps_json LONGTEXT NOT NULL,
    next_timeout_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    version BIGINT NOT NULL
);

CREATE INDEX idx_lsf_saga_corr ON lsf_saga_instance (correlation_id);
CREATE INDEX idx_lsf_saga_timeout ON lsf_saga_instance (next_timeout_at_ms);
