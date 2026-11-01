CREATE TABLE tenants (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE destinations (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id),
    code text NOT NULL,
    url text NOT NULL,
    key_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT destinations_tenant_code_key UNIQUE (tenant_id, code),
    CONSTRAINT destinations_tenant_id_key UNIQUE (tenant_id, id)
);

CREATE TABLE events (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id),
    idempotency_key text NOT NULL,
    request_body bytea NOT NULL,
    request_hash bytea NOT NULL,
    event_type text NOT NULL,
    wire_body bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT events_tenant_id_key UNIQUE (tenant_id, id),
    CONSTRAINT events_tenant_idempotency_key UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT events_request_hash_length_check CHECK (octet_length(request_hash) = 32),
    CONSTRAINT events_request_body_size_check CHECK (octet_length(request_body) <= 65536),
    CONSTRAINT events_wire_body_size_check CHECK (octet_length(wire_body) <= 98304),
    CONSTRAINT events_idempotency_key_length_check CHECK (char_length(idempotency_key) BETWEEN 1 AND 128)
);

CREATE INDEX events_tenant_created_id_idx
    ON events (tenant_id, created_at DESC, id DESC);

CREATE TABLE deliveries (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    event_id uuid NOT NULL,
    destination_id uuid NOT NULL,
    url_snapshot text NOT NULL,
    key_ref_snapshot text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    cycle_no integer NOT NULL DEFAULT 1,
    cycle_attempt_count integer NOT NULL DEFAULT 0,
    lifetime_attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5,
    next_attempt_at timestamptz NOT NULL,
    generation bigint NOT NULL DEFAULT 0,
    claim_token uuid,
    lease_until timestamptz,
    confirmed_at timestamptz,
    terminal_reason text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deliveries_event_tenant_fk
        FOREIGN KEY (tenant_id, event_id) REFERENCES events (tenant_id, id),
    CONSTRAINT deliveries_destination_tenant_fk
        FOREIGN KEY (tenant_id, destination_id) REFERENCES destinations (tenant_id, id),
    CONSTRAINT deliveries_event_destination_key UNIQUE (event_id, destination_id),
    CONSTRAINT deliveries_tenant_id_key UNIQUE (tenant_id, id),
    CONSTRAINT deliveries_status_check
        CHECK (status IN ('pending', 'in_flight', 'succeeded', 'dead')),
    CONSTRAINT deliveries_cycle_no_check CHECK (cycle_no >= 1),
    CONSTRAINT deliveries_cycle_attempt_count_check CHECK (cycle_attempt_count >= 0),
    CONSTRAINT deliveries_lifetime_attempt_count_check CHECK (lifetime_attempt_count >= 0),
    CONSTRAINT deliveries_generation_check CHECK (generation >= 0),
    CONSTRAINT deliveries_max_attempts_check CHECK (max_attempts = 5),
    CONSTRAINT deliveries_cycle_attempt_limit_check CHECK (cycle_attempt_count <= max_attempts),
    CONSTRAINT deliveries_lifetime_attempt_count_order_check
        CHECK (lifetime_attempt_count >= cycle_attempt_count),
    CONSTRAINT deliveries_claim_lease_state_check CHECK (
        (status = 'in_flight' AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
        OR
        (status <> 'in_flight' AND claim_token IS NULL AND lease_until IS NULL)
    ),
    CONSTRAINT deliveries_confirmed_state_check CHECK (
        (status = 'succeeded' AND confirmed_at IS NOT NULL)
        OR
        (status <> 'succeeded' AND confirmed_at IS NULL)
    ),
    CONSTRAINT deliveries_terminal_reason_state_check CHECK (
        (status = 'dead' AND terminal_reason IS NOT NULL)
        OR
        (status <> 'dead' AND terminal_reason IS NULL)
    )
);

CREATE INDEX deliveries_pending_due_idx
    ON deliveries (next_attempt_at, id)
    WHERE status = 'pending';

CREATE INDEX deliveries_in_flight_expired_idx
    ON deliveries (lease_until, id)
    WHERE status = 'in_flight';

CREATE INDEX deliveries_tenant_event_id_idx
    ON deliveries (tenant_id, event_id, id);

CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries (id),
    attempt_no integer NOT NULL,
    cycle_no integer NOT NULL,
    cycle_attempt_no integer NOT NULL,
    generation bigint NOT NULL,
    claim_token uuid NOT NULL,
    outcome text NOT NULL DEFAULT 'running',
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamptz,
    http_status integer,
    error_code text,
    duration_ms bigint,
    retry_delay_ms bigint,
    CONSTRAINT attempts_delivery_attempt_no_key UNIQUE (delivery_id, attempt_no),
    CONSTRAINT attempts_delivery_generation_key UNIQUE (delivery_id, generation),
    CONSTRAINT attempts_attempt_no_check CHECK (attempt_no >= 1),
    CONSTRAINT attempts_cycle_no_check CHECK (cycle_no >= 1),
    CONSTRAINT attempts_cycle_attempt_no_check CHECK (cycle_attempt_no >= 1),
    CONSTRAINT attempts_outcome_check
        CHECK (outcome IN ('running', 'acknowledged', 'http_failure', 'unconfirmed', 'local_failure')),
    CONSTRAINT attempts_finished_state_check CHECK (
        (outcome = 'running' AND finished_at IS NULL)
        OR
        (outcome <> 'running' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT attempts_http_status_range_check
        CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    CONSTRAINT attempts_acknowledged_status_check CHECK (
        outcome <> 'acknowledged'
        OR (http_status IS NOT NULL AND http_status BETWEEN 200 AND 299)
    ),
    CONSTRAINT attempts_http_failure_status_check CHECK (
        outcome <> 'http_failure'
        OR (http_status IS NOT NULL AND (http_status < 200 OR http_status > 299))
    ),
    CONSTRAINT attempts_duration_ms_check CHECK (duration_ms IS NULL OR duration_ms >= 0),
    CONSTRAINT attempts_retry_delay_ms_check CHECK (retry_delay_ms IS NULL OR retry_delay_ms >= 0)
);

CREATE TABLE replays (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id),
    delivery_id uuid NOT NULL,
    request_key text NOT NULL,
    actor text NOT NULL,
    from_cycle integer NOT NULL,
    to_cycle integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT replays_delivery_tenant_fk
        FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries (tenant_id, id),
    CONSTRAINT replays_tenant_request_key UNIQUE (tenant_id, request_key),
    CONSTRAINT replays_delivery_to_cycle_key UNIQUE (delivery_id, to_cycle),
    CONSTRAINT replays_from_cycle_check CHECK (from_cycle >= 1),
    CONSTRAINT replays_to_cycle_check CHECK (to_cycle = from_cycle + 1)
);

CREATE TABLE demo_scenarios (
    receiver_code text NOT NULL,
    scenario_id text NOT NULL,
    mode text NOT NULL,
    dedupe_enabled boolean NOT NULL,
    drop_remaining integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT demo_scenarios_pkey PRIMARY KEY (receiver_code, scenario_id),
    CONSTRAINT demo_scenarios_receiver_code_check
        CHECK (receiver_code IN ('fulfillment', 'notification')),
    CONSTRAINT demo_scenarios_id_check CHECK (
        char_length(scenario_id) BETWEEN 1 AND 64
        AND scenario_id !~ '[^A-Za-z0-9_-]'
    ),
    CONSTRAINT demo_scenarios_mode_check
        CHECK (mode IN ('normal', 'http_503', 'http_429', 'commit_drop_once')),
    CONSTRAINT demo_scenarios_drop_remaining_check CHECK (drop_remaining IN (0, 1))
);

CREATE TABLE demo_requests (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    receiver_code text NOT NULL,
    scenario_id text NOT NULL,
    operation_id uuid NOT NULL,
    received_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    planned_outcome text NOT NULL,
    business_effect_added boolean NOT NULL,
    CONSTRAINT demo_requests_scenario_fk
        FOREIGN KEY (receiver_code, scenario_id)
        REFERENCES demo_scenarios (receiver_code, scenario_id),
    CONSTRAINT demo_requests_planned_outcome_check
        CHECK (planned_outcome IN ('acknowledge', 'reject_503', 'reject_429', 'drop', 'conflict'))
);

CREATE TABLE demo_dedup (
    receiver_code text NOT NULL,
    scenario_id text NOT NULL,
    operation_id uuid NOT NULL,
    wire_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT demo_dedup_pkey PRIMARY KEY (receiver_code, scenario_id, operation_id),
    CONSTRAINT demo_dedup_scenario_fk
        FOREIGN KEY (receiver_code, scenario_id)
        REFERENCES demo_scenarios (receiver_code, scenario_id),
    CONSTRAINT demo_dedup_wire_hash_length_check CHECK (octet_length(wire_hash) = 32)
);

CREATE TABLE demo_effects (
    id uuid PRIMARY KEY,
    receiver_code text NOT NULL,
    scenario_id text NOT NULL,
    operation_id uuid NOT NULL,
    order_id text NOT NULL,
    quantity integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT demo_effects_scenario_fk
        FOREIGN KEY (receiver_code, scenario_id)
        REFERENCES demo_scenarios (receiver_code, scenario_id)
);
