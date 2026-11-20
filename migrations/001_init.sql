BEGIN;

CREATE TABLE user_profiles (
    id text PRIMARY KEY,
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PENDING_DELETION'))
);
CREATE TABLE addresses (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES user_profiles(id),
    address_line_1 text NOT NULL CHECK (address_line_1 <> ''),
    address_line_2 text,
    city text NOT NULL CHECK (city <> ''),
    state text NOT NULL CHECK (state <> ''),
    zip_code text NOT NULL CHECK (zip_code <> ''),
    active boolean NOT NULL DEFAULT true,
    type text NOT NULL DEFAULT 'HOME' CHECK (type IN ('HOME', 'WORK', 'MAILING', 'BILLING'))
);
CREATE TABLE contact_methods (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES user_profiles(id),
    value text NOT NULL
);
-- Fingerprints outlive deletable metadata; an allocated storage address is never reused.
CREATE TABLE storage_object_identities (
    identity_hash bytea PRIMARY KEY CHECK (octet_length(identity_hash) = 32)
);
CREATE TABLE object_metadata (
    object_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES user_profiles(id),
    bucket text NOT NULL,
    prefix text NOT NULL,
    filename text NOT NULL,
    object_key text GENERATED ALWAYS AS (prefix || filename) STORED,
    original_filename text NOT NULL,
    content_type text NOT NULL,
    size bigint NOT NULL CHECK (size >= 0),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PENDING_DELETION')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket, object_key)
);
CREATE INDEX ON object_metadata(user_id, object_id);
CREATE INDEX ON addresses(user_id, id);
CREATE INDEX ON contact_methods(user_id, id);

CREATE TABLE deletion_requests (
    request_id uuid PRIMARY KEY,
    user_hash varchar(64) NOT NULL,
    initiator_user_hash varchar(64) NOT NULL,
    user_id text,
    traceparent text NOT NULL,
    tracestate text NOT NULL DEFAULT '',
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'COMPLETED')),
    enumeration_complete boolean NOT NULL DEFAULT false,
    cursor bigint NOT NULL DEFAULT 0 CHECK (cursor >= 0),
    finalizer_generation integer NOT NULL DEFAULT 0,
    finalizer_dispatched_at timestamptz,
    recovery_count integer NOT NULL DEFAULT 0,
    db_deleted_count bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_progress_at timestamptz NOT NULL DEFAULT now(),
    next_reconcile_at timestamptz NOT NULL DEFAULT now() + interval '15 minutes',
    completed_at timestamptz,
    CHECK ((state = 'COMPLETED') = (completed_at IS NOT NULL)),
    CHECK (state <> 'COMPLETED' OR (user_id IS NULL AND enumeration_complete))
);
CREATE UNIQUE INDEX one_pending_request_per_user ON deletion_requests(user_hash) WHERE state = 'PENDING';
CREATE INDEX ON deletion_requests(next_reconcile_at, request_id) WHERE state = 'PENDING';
CREATE INDEX ON deletion_requests(created_at) WHERE state = 'PENDING';
CREATE TABLE deletion_batches (
    batch_id uuid PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES deletion_requests(request_id),
    start_key bigint NOT NULL CHECK (start_key >= 0),
    next_key bigint,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'BLOCKED', 'COMPLETED')),
    attempts integer NOT NULL DEFAULT 0,
    recovery_count integer NOT NULL DEFAULT 0,
    deleted_count bigint NOT NULL DEFAULT 0 CHECK (deleted_count >= 0),
    lease_token uuid,
    lease_until timestamptz,
    last_dispatched_at timestamptz NOT NULL DEFAULT now(),
    last_progress_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (request_id, start_key),
    UNIQUE (request_id, batch_id),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX ON deletion_batches(request_id, state, last_dispatched_at);
CREATE TABLE deletion_batch_items (
    batch_id uuid NOT NULL REFERENCES deletion_batches(batch_id),
    object_id bigint NOT NULL,
    bucket text,
    object_key text,
    prefix text,
    filename text,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'DELETED', 'INVALID', 'BLOCKED')),
    attempts integer NOT NULL DEFAULT 0,
    error_code text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, object_id),
    CHECK (state = 'DELETED' OR (bucket IS NOT NULL AND object_key IS NOT NULL AND prefix IS NOT NULL AND filename IS NOT NULL))
);
CREATE TABLE deletion_outbox (
    id uuid PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES deletion_requests(request_id),
    batch_id uuid REFERENCES deletion_batches(batch_id),
    destination text NOT NULL CHECK (destination IN ('db', 's3')),
    payload jsonb NOT NULL,
    sent_at timestamptz,
    available_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0,
    lease_token uuid,
    lease_until timestamptz,
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX ON deletion_outbox(destination, available_at, created_at, id) WHERE sent_at IS NULL;
CREATE INDEX ON deletion_outbox(request_id, batch_id) WHERE sent_at IS NULL;
CREATE INDEX ON deletion_outbox(created_at) WHERE sent_at IS NULL;
CREATE INDEX ON deletion_outbox(sent_at, id) WHERE sent_at IS NOT NULL;
CREATE TABLE deletion_audit_log (
    audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    deletion_request_id uuid NOT NULL REFERENCES deletion_requests(request_id),
    user_id_hash varchar(64) NOT NULL,
    initiator_user_hash varchar(64) NOT NULL,
    trace_id varchar(32) NOT NULL,
    stage text NOT NULL CHECK (stage IN ('INITIATED', 'BATCH_COMPLETED', 'COMPLETED')),
    resource_type text NOT NULL,
    items_affected bigint NOT NULL DEFAULT 0 CHECK (items_affected >= 0),
    batch_id uuid REFERENCES deletion_batches(batch_id),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX ON deletion_audit_log(deletion_request_id, stage) WHERE batch_id IS NULL;
CREATE UNIQUE INDEX ON deletion_audit_log(batch_id) WHERE batch_id IS NOT NULL;
CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'deletion ledger is append-only';
END;
$$;
CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE OR TRUNCATE ON deletion_audit_log
FOR EACH STATEMENT EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER immutable_storage_identities BEFORE UPDATE OR DELETE OR TRUNCATE ON storage_object_identities
FOR EACH STATEMENT EXECUTE FUNCTION reject_ledger_mutation();

CREATE FUNCTION guard_user_data() RETURNS trigger LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.user_id IS DISTINCT FROM OLD.user_id THEN
        RAISE EXCEPTION 'ownership is immutable';
    END IF;
    IF TG_TABLE_NAME = 'object_metadata' THEN
        IF TG_OP = 'UPDATE' THEN
            IF NEW.object_id IS DISTINCT FROM OLD.object_id OR NEW.bucket IS DISTINCT FROM OLD.bucket
                OR NEW.prefix IS DISTINCT FROM OLD.prefix OR NEW.filename IS DISTINCT FROM OLD.filename THEN
                RAISE EXCEPTION 'storage identity is immutable';
            END IF;
            -- The runtime has column-level UPDATE permission only for this status.
            IF session_user = 'deletion_worker' AND NEW.status = 'PENDING_DELETION' THEN
                RETURN NEW;
            END IF;
        ELSE
            INSERT INTO public.storage_object_identities(identity_hash)
            VALUES (sha256(convert_to(length(NEW.bucket)::text || ':' || NEW.bucket || NEW.prefix || NEW.filename, 'UTF8')));
        END IF;
    END IF;
    PERFORM 1 FROM public.user_profiles WHERE id = NEW.user_id AND status = 'ACTIVE' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'user is unavailable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_objects BEFORE INSERT OR UPDATE ON object_metadata
FOR EACH ROW EXECUTE FUNCTION guard_user_data();
CREATE TRIGGER guard_addresses BEFORE INSERT OR UPDATE ON addresses
FOR EACH ROW EXECUTE FUNCTION guard_user_data();
CREATE TRIGGER guard_contacts BEFORE INSERT OR UPDATE ON contact_methods
FOR EACH ROW EXECUTE FUNCTION guard_user_data();

REVOKE ALL ON deletion_audit_log, storage_object_identities FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'deletion_worker') THEN
        GRANT USAGE ON SCHEMA public TO deletion_worker;
        REVOKE CREATE ON SCHEMA public FROM deletion_worker;
        GRANT SELECT, DELETE ON user_profiles TO deletion_worker;
        GRANT UPDATE(status) ON user_profiles TO deletion_worker;
        GRANT SELECT, DELETE ON addresses, contact_methods, object_metadata TO deletion_worker;
        GRANT UPDATE(status) ON object_metadata TO deletion_worker;
        GRANT SELECT, INSERT, UPDATE ON deletion_requests, deletion_batches, deletion_batch_items, deletion_outbox TO deletion_worker;
        GRANT SELECT, INSERT ON deletion_audit_log TO deletion_worker;
        REVOKE UPDATE, DELETE, TRUNCATE ON deletion_audit_log FROM deletion_worker;
    END IF;
END;
$$;
COMMIT;
