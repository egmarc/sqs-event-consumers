BEGIN;

CREATE TABLE users (
    id text PRIMARY KEY,
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PENDING_DELETION'))
);
CREATE TABLE address (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id),
    address text NOT NULL
);
CREATE TABLE contact_methods (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id text NOT NULL REFERENCES users(id),
    value text NOT NULL
);
CREATE TABLE object_storage (
    user_id text NOT NULL REFERENCES users(id),
    object_key text PRIMARY KEY
);
CREATE INDEX ON object_storage(user_id);
CREATE INDEX ON address(user_id);
CREATE INDEX ON contact_methods(user_id);

-- Requests retain only a salted identifier, not the deleted user's identifier.
CREATE TABLE deletion_requests (
    request_id uuid PRIMARY KEY,
    user_hash text NOT NULL,
    user_id text,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'COMPLETED')),
    finalizer_enqueued boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE UNIQUE INDEX one_pending_request_per_user ON deletion_requests(user_hash) WHERE state = 'PENDING';
CREATE TABLE deletion_batches (
    batch_id uuid PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES deletion_requests(request_id),
    keys text[] NOT NULL,
    completed boolean NOT NULL DEFAULT false,
    deleted_count integer NOT NULL DEFAULT 0 CHECK (deleted_count >= 0),
    CHECK ((NOT completed AND cardinality(keys) BETWEEN 1 AND 1000) OR (completed AND cardinality(keys) = 0)),
    UNIQUE (request_id, batch_id)
);
CREATE INDEX ON deletion_batches(request_id);
CREATE TABLE deletion_outbox (
    id uuid PRIMARY KEY,
    destination text NOT NULL CHECK (destination IN ('db', 's3')),
    payload jsonb NOT NULL,
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON deletion_outbox(created_at) WHERE sent_at IS NULL;
CREATE TABLE deletion_audit_log (
    audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    deletion_request_id uuid NOT NULL REFERENCES deletion_requests(request_id),
    user_id_hash varchar(64) NOT NULL,
    stage text NOT NULL CHECK (stage IN ('INITIATED', 'BATCH_COMPLETED', 'COMPLETED')),
    resource_type text NOT NULL,
    items_affected integer NOT NULL DEFAULT 0,
    metadata jsonb NOT NULL DEFAULT '{}',
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

-- Metadata inserts and ownership changes must serialize with deletion initiation.
CREATE FUNCTION guard_user_data() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.user_id IS DISTINCT FROM OLD.user_id THEN
        RAISE EXCEPTION 'ownership is immutable';
    END IF;
    IF TG_TABLE_NAME = 'object_storage' THEN
        IF TG_OP = 'UPDATE' AND NEW.object_key IS DISTINCT FROM OLD.object_key THEN
            RAISE EXCEPTION 'object keys are immutable';
        END IF;
        IF EXISTS (SELECT 1 FROM deletion_batches WHERE NEW.object_key = ANY(keys)) THEN
            RAISE EXCEPTION 'object key has been retired';
        END IF;
    END IF;
    PERFORM 1 FROM users WHERE id = NEW.user_id AND status = 'ACTIVE' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'user is unavailable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER guard_objects BEFORE INSERT OR UPDATE ON object_storage
FOR EACH ROW EXECUTE FUNCTION guard_user_data();
CREATE TRIGGER guard_address BEFORE INSERT OR UPDATE ON address
FOR EACH ROW EXECUTE FUNCTION guard_user_data();
CREATE TRIGGER guard_contacts BEFORE INSERT OR UPDATE ON contact_methods
FOR EACH ROW EXECUTE FUNCTION guard_user_data();

REVOKE ALL ON deletion_audit_log FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'deletion_worker') THEN
        GRANT USAGE ON SCHEMA public TO deletion_worker;
        REVOKE CREATE ON SCHEMA public FROM deletion_worker;
        GRANT SELECT, UPDATE, DELETE ON users TO deletion_worker;
        GRANT SELECT, DELETE ON address, contact_methods, object_storage TO deletion_worker;
        GRANT SELECT, INSERT, UPDATE ON deletion_requests, deletion_batches, deletion_outbox TO deletion_worker;
        GRANT SELECT, INSERT ON deletion_audit_log TO deletion_worker;
        REVOKE UPDATE, DELETE, TRUNCATE ON deletion_audit_log FROM deletion_worker;
    END IF;
END;
$$;
COMMIT;
