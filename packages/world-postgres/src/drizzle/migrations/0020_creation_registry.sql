CREATE TABLE workflow.creation_operations (
  operation_key text PRIMARY KEY,
  intent_hash text NOT NULL,
  parent_operation_key text REFERENCES workflow.creation_operations(operation_key),
  source_operation_key text REFERENCES workflow.creation_operations(operation_key),
  canonical_run_id text,
  protocol_version integer NOT NULL CHECK (protocol_version = 1),
  state text NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'sealed', 'retired')),
  sealed_at timestamptz,
  retired_at timestamptz
);
CREATE TABLE workflow.creation_candidates (
  run_id text PRIMARY KEY,
  operation_key text NOT NULL REFERENCES workflow.creation_operations(operation_key),
  role text NOT NULL CHECK (role IN ('session', 'auxiliary')),
  claim_token text,
  lineage jsonb NOT NULL,
  owner_claimed boolean NOT NULL DEFAULT false
);
CREATE INDEX creation_candidates_operation ON workflow.creation_candidates(operation_key);
CREATE TABLE workflow.creation_streams (
  stream_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES workflow.creation_candidates(run_id)
);

CREATE FUNCTION workflow.creation_family_root(op_key text) RETURNS text LANGUAGE sql STABLE AS $$
  WITH RECURSIVE ancestors AS (
    SELECT operation_key,parent_operation_key FROM workflow.creation_operations WHERE operation_key=op_key
    UNION ALL
    SELECT o.operation_key,o.parent_operation_key FROM workflow.creation_operations o
    JOIN ancestors a ON o.operation_key=a.parent_operation_key
  ) SELECT coalesce((SELECT operation_key FROM ancestors WHERE parent_operation_key IS NULL),op_key)
$$;
CREATE FUNCTION workflow.lock_creation_family(op_key text) RETURNS void LANGUAGE sql AS $$
  SELECT pg_advisory_xact_lock(hashtextextended(workflow.creation_family_root(op_key),652))
$$;

CREATE FUNCTION workflow.lock_creation_dependencies(op_key text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE lock_key text;
BEGIN
  FOR lock_key IN SELECT DISTINCT value FROM unnest(ARRAY[
    workflow.creation_family_root(op_key),
    (SELECT workflow.creation_family_root(source_operation_key) FROM workflow.creation_operations
      WHERE operation_key=op_key AND source_operation_key IS NOT NULL)
  ]) AS value WHERE value IS NOT NULL ORDER BY value LOOP
    PERFORM workflow.lock_creation_family(lock_key);
  END LOOP;
END $$;

-- The family lock precedes row locks in every write/claim/seal path. It closes
-- owned-descendant admission while seal snapshots the entire durable closure.
CREATE FUNCTION workflow.admit_creation_candidate(candidate_id text, attrs jsonb, native_name text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  op_key text := attrs->>'$eve.creation.key';
  intent text := attrs->>'$eve.creation.intent';
  candidate_role text := attrs->>'$eve.creation.role';
  token text := attrs->>'$eve.creation.claim_token';
  native_lineage jsonb := jsonb_build_object(
    'parentRunId',attrs->>'$eve.parent','rootRunId',attrs->>'$eve.root',
    'parentTurnId',attrs->>'$eve.parent_turn','parentCallId',attrs->>'$eve.parent_call');
  parent_key text;
  source_key text;
  lineage_run text;
  lineage_key text;
  family_root text;
  lock_key text;
  op workflow.creation_operations%ROWTYPE;
  candidate workflow.creation_candidates%ROWTYPE;
BEGIN
  IF candidate_id IS NULL OR candidate_id = '' THEN
    RAISE EXCEPTION 'Candidate run identity is required' USING ERRCODE = '23514';
  END IF;
  SELECT * INTO candidate FROM workflow.creation_candidates WHERE run_id=candidate_id;
  IF candidate.run_id IS NOT NULL THEN
    IF attrs IS NOT NULL AND candidate.lineage IS DISTINCT FROM native_lineage THEN
      RAISE EXCEPTION 'Candidate lineage is immutable' USING ERRCODE='23514';
    END IF;
    native_lineage:=candidate.lineage;
    IF op_key IS NOT NULL AND op_key<>candidate.operation_key THEN
      RAISE EXCEPTION 'Candidate identity is immutable' USING ERRCODE='23514';
    END IF;
    op_key:=candidate.operation_key;
    SELECT parent_operation_key,source_operation_key INTO parent_key,source_key
      FROM workflow.creation_operations WHERE operation_key=op_key;
  ELSE
    -- Ordinary Workflow families do not opt into eve ownership. Generic
    -- descendants of a tracked candidate still inherit its admission fence.
    IF op_key IS NULL AND coalesce(native_name,'') NOT LIKE 'workflow//eve%'
       AND NOT coalesce(attrs ?| ARRAY['$eve.type','$eve.parent','$eve.root',
         '$eve.creation.intent','$eve.creation.role','$eve.creation.claim_token'],false)
       AND NOT EXISTS (
         SELECT 1 FROM workflow.creation_candidates WHERE run_id = ANY(ARRAY[
           attrs->>'$parentRunId',attrs->>'$rootRunId',attrs->>'$eve.parent',attrs->>'$eve.root'])
       ) THEN
      RETURN;
    END IF;
    FOREACH lineage_run IN ARRAY ARRAY[attrs->>'$parentRunId',attrs->>'$eve.parent',attrs->>'$rootRunId',attrs->>'$eve.root'] LOOP
      IF lineage_run IS NULL THEN CONTINUE; END IF;
      SELECT operation_key INTO lineage_key FROM workflow.creation_candidates WHERE run_id=lineage_run;
      IF lineage_key IS NULL THEN
        RAISE EXCEPTION 'Parent candidate inventory is incomplete' USING ERRCODE='23514';
      END IF;
      IF family_root IS NOT NULL AND family_root<>workflow.creation_family_root(lineage_key) THEN
        RAISE EXCEPTION 'Conflicting candidate lineage' USING ERRCODE='23514';
      END IF;
      family_root:=workflow.creation_family_root(lineage_key);
      parent_key:=coalesce(parent_key,lineage_key);
    END LOOP;
    IF attrs->>'$eve.creation.source_run' IS NOT NULL THEN
      SELECT operation_key INTO source_key FROM workflow.creation_candidates
        WHERE run_id=attrs->>'$eve.creation.source_run';
      IF source_key IS NULL THEN
        RAISE EXCEPTION 'Source candidate inventory is incomplete' USING ERRCODE='23514';
      END IF;
    END IF;
    IF parent_key IS NOT NULL AND candidate_role IS DISTINCT FROM 'session' THEN
      IF op_key IS NOT NULL AND op_key<>parent_key THEN
        RAISE EXCEPTION 'Auxiliary candidate operation mismatch' USING ERRCODE='23514';
      END IF;
      op_key:=parent_key;
      SELECT intent_hash,parent_operation_key,source_operation_key INTO intent,parent_key,source_key
        FROM workflow.creation_operations WHERE operation_key=op_key;
      candidate_role:='auxiliary'; token:=NULL;
    END IF;
  END IF;
  IF op_key IS NULL THEN
    IF native_name LIKE 'workflow//eve%' OR attrs ? '$eve.type' THEN
      RAISE EXCEPTION 'Native eve creation requires a trusted candidate envelope' USING ERRCODE='23514';
    END IF;
    RETURN;
  END IF;
  IF op_key=parent_key OR op_key=source_key THEN
    RAISE EXCEPTION 'Candidate operation cannot depend on itself' USING ERRCODE='23514';
  END IF;
  -- Source dependencies do not enter erase-family closure. Order the two
  -- family locks so cross-family copy admission cannot deadlock.
  FOR lock_key IN SELECT DISTINCT value FROM unnest(ARRAY[
    workflow.creation_family_root(coalesce(parent_key,op_key)),
    CASE WHEN source_key IS NULL THEN NULL ELSE workflow.creation_family_root(source_key) END
  ]) AS value WHERE value IS NOT NULL ORDER BY value LOOP
    PERFORM workflow.lock_creation_family(lock_key);
  END LOOP;
  IF EXISTS (
    WITH RECURSIVE ancestors AS (
      SELECT operation_key,parent_operation_key,state FROM workflow.creation_operations
        WHERE operation_key IN (parent_key,source_key)
      UNION ALL SELECT o.operation_key,o.parent_operation_key,o.state FROM workflow.creation_operations o
        JOIN ancestors a ON o.operation_key=a.parent_operation_key
    ) SELECT 1 FROM ancestors WHERE state<>'open'
  ) AND candidate.run_id IS NULL THEN
    RAISE EXCEPTION 'Candidate ancestor or source is sealed' USING ERRCODE='23514';
  END IF;
  IF candidate.run_id IS NULL THEN
    IF length(op_key) NOT BETWEEN 1 AND 256 OR intent !~ '^[a-f0-9]{64}$'
       OR candidate_role NOT IN ('session','auxiliary') OR intent IS NULL OR candidate_role IS NULL
       OR (candidate_role='session' AND coalesce(token,'')='') THEN
      RAISE EXCEPTION 'Invalid candidate envelope' USING ERRCODE='23514';
    END IF;
    INSERT INTO workflow.creation_operations(operation_key,intent_hash,protocol_version,parent_operation_key,source_operation_key)
      VALUES(op_key,intent,1,parent_key,source_key) ON CONFLICT DO NOTHING;
  END IF;
  SELECT * INTO op FROM workflow.creation_operations WHERE operation_key=op_key FOR UPDATE;
  SELECT * INTO candidate FROM workflow.creation_candidates WHERE run_id=candidate_id;
  IF op.operation_key IS NULL OR op.protocol_version<>1 THEN
    RAISE EXCEPTION 'Candidate operation protocol is missing or unknown' USING ERRCODE='23514';
  END IF;
  IF op.state='retired' OR (op.state='sealed' AND candidate.run_id IS NULL) THEN
    RAISE EXCEPTION 'Candidate operation is sealed' USING ERRCODE='23514';
  END IF;
  IF (intent IS NOT NULL AND op.intent_hash<>intent)
     OR op.parent_operation_key IS DISTINCT FROM parent_key
     OR op.source_operation_key IS DISTINCT FROM source_key THEN
    RAISE EXCEPTION 'Candidate operation intent mismatch' USING ERRCODE='23514';
  END IF;
  INSERT INTO workflow.creation_candidates(run_id,operation_key,role,claim_token,lineage)
    VALUES(candidate_id,op_key,coalesce(candidate_role,candidate.role),coalesce(token,candidate.claim_token),native_lineage)
    ON CONFLICT DO NOTHING;
  SELECT * INTO candidate FROM workflow.creation_candidates WHERE run_id=candidate_id;
  IF candidate.operation_key<>op_key
     OR candidate.lineage IS DISTINCT FROM native_lineage
     OR (candidate_role IS NOT NULL AND candidate.role<>candidate_role)
     OR (token IS NOT NULL AND candidate.claim_token IS DISTINCT FROM token) THEN
    RAISE EXCEPTION 'Candidate identity is immutable' USING ERRCODE='23514';
  END IF;
END $$;

CREATE FUNCTION workflow.guard_creation_payload(candidate_id text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE op_state text; op_key text;
BEGIN
  SELECT operation_key INTO op_key FROM workflow.creation_candidates WHERE run_id=candidate_id;
  IF op_key IS NOT NULL THEN PERFORM workflow.lock_creation_family(op_key); END IF;
  SELECT o.state INTO op_state FROM workflow.creation_operations o
    JOIN workflow.creation_candidates c USING(operation_key)
    WHERE c.run_id = candidate_id FOR UPDATE OF o;
  IF op_state = 'retired' THEN
    RAISE EXCEPTION 'Candidate operation payloads are retired' USING ERRCODE = '23514';
  END IF;
END $$;

CREATE FUNCTION workflow.guard_creation_run() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM workflow.admit_creation_candidate(NEW.id,NEW.attributes,NEW.name);
  ELSE
    IF NEW.id IS DISTINCT FROM OLD.id THEN
      RAISE EXCEPTION 'Run identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM workflow.creation_candidates WHERE run_id = NEW.id)
       AND (NEW.attributes->'$eve.creation.key' IS DISTINCT FROM OLD.attributes->'$eve.creation.key'
         OR NEW.attributes->'$eve.creation.intent' IS DISTINCT FROM OLD.attributes->'$eve.creation.intent'
         OR NEW.attributes->'$eve.creation.role' IS DISTINCT FROM OLD.attributes->'$eve.creation.role'
         OR NEW.attributes->'$eve.creation.claim_token' IS DISTINCT FROM OLD.attributes->'$eve.creation.claim_token'
         OR NEW.attributes->'$eve.parent' IS DISTINCT FROM OLD.attributes->'$eve.parent'
         OR NEW.attributes->'$eve.root' IS DISTINCT FROM OLD.attributes->'$eve.root'
         OR NEW.attributes->'$eve.parent_turn' IS DISTINCT FROM OLD.attributes->'$eve.parent_turn'
         OR NEW.attributes->'$eve.parent_call' IS DISTINCT FROM OLD.attributes->'$eve.parent_call') THEN
      RAISE EXCEPTION 'Candidate attributes are immutable' USING ERRCODE = '23514';
    END IF;
  END IF;
  PERFORM workflow.guard_creation_payload(NEW.id);
  RETURN NEW;
END $$;
CREATE TRIGGER creation_run_guard BEFORE INSERT OR UPDATE ON workflow.workflow_runs
  FOR EACH ROW EXECUTE FUNCTION workflow.guard_creation_run();

CREATE FUNCTION workflow.guard_creation_child() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE stream_owner text; hook_operation text;
BEGIN
  IF TG_TABLE_NAME = 'workflow_hooks' THEN
    SELECT operation_key INTO hook_operation FROM workflow.creation_candidates
      WHERE run_id=NEW.run_id AND claim_token=NEW.token;
    IF hook_operation IS NOT NULL THEN
      PERFORM workflow.lock_creation_dependencies(hook_operation);
    END IF;
  END IF;
  IF TG_TABLE_NAME = 'workflow_stream_chunks' THEN
    SELECT run_id INTO stream_owner FROM workflow.creation_streams WHERE stream_id = NEW.stream_id;
    IF stream_owner IS NOT NULL AND NEW.run_id IS DISTINCT FROM stream_owner THEN
      RAISE EXCEPTION 'Stream candidate identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (SELECT 1 FROM workflow.creation_candidates WHERE run_id = NEW.run_id) THEN
      PERFORM workflow.guard_creation_payload(NEW.run_id);
      INSERT INTO workflow.creation_streams(stream_id,run_id) VALUES(NEW.stream_id,NEW.run_id)
        ON CONFLICT DO NOTHING;
      SELECT run_id INTO stream_owner FROM workflow.creation_streams WHERE stream_id = NEW.stream_id;
      IF stream_owner IS DISTINCT FROM NEW.run_id THEN
        RAISE EXCEPTION 'Stream candidate identity is immutable' USING ERRCODE = '23514';
      END IF;
    END IF;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.run_id IS DISTINCT FROM OLD.run_id THEN
    RAISE EXCEPTION 'Payload run identity is immutable' USING ERRCODE = '23514';
  END IF;
  PERFORM workflow.guard_creation_payload(NEW.run_id);
  RETURN NEW;
END $$;
DO $$ DECLARE relation text; BEGIN
  FOREACH relation IN ARRAY ARRAY['workflow_events','workflow_event_slots','workflow_steps','workflow_hooks','workflow_waits','workflow_stream_chunks'] LOOP
    EXECUTE format('CREATE TRIGGER creation_payload_guard BEFORE INSERT OR UPDATE ON workflow.%I FOR EACH ROW EXECUTE FUNCTION workflow.guard_creation_child()', relation);
  END LOOP;
END $$;

-- Canonical selection is committed with the actual continuation hook claim.
-- A conflicting hook INSERT rolls this update back too.
CREATE FUNCTION workflow.claim_creation_owner() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE candidate workflow.creation_candidates%ROWTYPE; op workflow.creation_operations%ROWTYPE;
BEGIN
  SELECT * INTO candidate FROM workflow.creation_candidates WHERE run_id = NEW.run_id;
  IF candidate.role = 'session' AND candidate.claim_token = NEW.token THEN
    PERFORM workflow.lock_creation_dependencies(candidate.operation_key);
    SELECT * INTO op FROM workflow.creation_operations WHERE operation_key = candidate.operation_key FOR UPDATE;
    IF EXISTS (
      WITH RECURSIVE ancestors AS (
        SELECT operation_key,parent_operation_key,state FROM workflow.creation_operations
          WHERE operation_key IN (op.parent_operation_key,op.source_operation_key)
        UNION ALL SELECT o.operation_key,o.parent_operation_key,o.state FROM workflow.creation_operations o
          JOIN ancestors a ON o.operation_key=a.parent_operation_key
      ) SELECT 1 FROM ancestors WHERE state<>'open'
    ) THEN
      RAISE EXCEPTION 'Candidate ancestor or source ownership is sealed' USING ERRCODE='23514';
    END IF;
    IF op.state <> 'open' OR (op.canonical_run_id IS NOT NULL AND op.canonical_run_id <> NEW.run_id) THEN
      RAISE EXCEPTION 'Candidate ownership is sealed or already claimed' USING ERRCODE = '23514';
    END IF;
    UPDATE workflow.creation_operations SET canonical_run_id = NEW.run_id WHERE operation_key = candidate.operation_key;
    UPDATE workflow.creation_candidates SET owner_claimed = true WHERE run_id = NEW.run_id;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER creation_owner_claim AFTER INSERT ON workflow.workflow_hooks
  FOR EACH ROW EXECUTE FUNCTION workflow.claim_creation_owner();

CREATE FUNCTION workflow.guard_creation_queue() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE candidate_id text := NEW.payload->>'runId'; attrs jsonb := NEW.payload::jsonb->'creationAttributes';
BEGIN
  IF coalesce((NEW.payload->>'healthCheck')::boolean,false) THEN RETURN NEW; END IF;
  IF candidate_id IS NOT NULL THEN
    IF coalesce((NEW.payload->>'initialRun')::boolean,false) THEN
      PERFORM workflow.admit_creation_candidate(candidate_id,attrs,NEW.payload->>'id');
    END IF;
    PERFORM workflow.guard_creation_payload(candidate_id);
  ELSIF NEW.payload::jsonb ? 'messageId' AND NEW.payload::jsonb ? 'data' AND NEW.payload::jsonb ? 'id' THEN
    RAISE EXCEPTION 'Workflow queue payload requires run identity' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;

-- Graphile is bootstrapped after Workflow on a fresh database. Install this
-- guard after that bootstrap, and also immediately on an existing database.
CREATE FUNCTION workflow.install_creation_queue_guard() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF to_regclass('graphile_worker._private_jobs') IS NULL THEN RETURN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'graphile_worker._private_jobs'::regclass AND tgname = 'creation_queue_guard') THEN
    CREATE TRIGGER creation_queue_guard BEFORE INSERT OR UPDATE OF payload ON graphile_worker._private_jobs
      FOR EACH ROW EXECUTE FUNCTION workflow.guard_creation_queue();
  END IF;
END $$;
SELECT workflow.install_creation_queue_guard();

-- Payload purge never removes the metadata needed to reject delayed writers.
CREATE FUNCTION workflow.protect_creation_operation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Creation tombstones must be retained' USING ERRCODE='23514'; END IF;
  IF NEW.operation_key IS DISTINCT FROM OLD.operation_key OR NEW.intent_hash IS DISTINCT FROM OLD.intent_hash
     OR NEW.protocol_version IS DISTINCT FROM OLD.protocol_version
     OR NEW.parent_operation_key IS DISTINCT FROM OLD.parent_operation_key
     OR NEW.source_operation_key IS DISTINCT FROM OLD.source_operation_key
     OR (OLD.canonical_run_id IS NOT NULL AND NEW.canonical_run_id IS DISTINCT FROM OLD.canonical_run_id)
     OR (OLD.state='sealed' AND NEW.state='open') OR (OLD.state='retired' AND NEW.state<>'retired') THEN
    RAISE EXCEPTION 'Creation operation identity and retirement are immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER creation_operation_immutable BEFORE UPDATE OR DELETE ON workflow.creation_operations
 FOR EACH ROW EXECUTE FUNCTION workflow.protect_creation_operation();
CREATE FUNCTION workflow.protect_creation_candidate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Creation associations must be retained' USING ERRCODE='23514'; END IF;
  IF NEW.run_id IS DISTINCT FROM OLD.run_id OR NEW.operation_key IS DISTINCT FROM OLD.operation_key
     OR NEW.role IS DISTINCT FROM OLD.role OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
     OR NEW.lineage IS DISTINCT FROM OLD.lineage
     OR (OLD.owner_claimed AND NOT NEW.owner_claimed) THEN
    RAISE EXCEPTION 'Creation candidate identity is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER creation_candidate_immutable BEFORE UPDATE OR DELETE ON workflow.creation_candidates
 FOR EACH ROW EXECUTE FUNCTION workflow.protect_creation_candidate();
CREATE FUNCTION workflow.protect_creation_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Creation stream associations must be retained' USING ERRCODE='23514';
END $$;
CREATE TRIGGER creation_stream_immutable BEFORE UPDATE OR DELETE ON workflow.creation_streams
 FOR EACH ROW EXECUTE FUNCTION workflow.protect_creation_stream();

CREATE FUNCTION workflow.assert_creation_registry_ready() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM (VALUES
      ('workflow.workflow_runs','creation_run_guard'),
      ('workflow.workflow_events','creation_payload_guard'),
      ('workflow.workflow_event_slots','creation_payload_guard'),
      ('workflow.workflow_steps','creation_payload_guard'),
      ('workflow.workflow_hooks','creation_payload_guard'),
      ('workflow.workflow_hooks','creation_owner_claim'),
      ('workflow.workflow_waits','creation_payload_guard'),
      ('workflow.workflow_stream_chunks','creation_payload_guard'),
      ('workflow.creation_operations','creation_operation_immutable'),
      ('workflow.creation_candidates','creation_candidate_immutable'),
      ('workflow.creation_streams','creation_stream_immutable'),
      ('graphile_worker._private_jobs','creation_queue_guard')
    ) AS required(relation,trigger_name)
    WHERE NOT EXISTS (SELECT 1 FROM pg_trigger t
      WHERE t.tgrelid=to_regclass(required.relation) AND t.tgname=required.trigger_name
      AND t.tgenabled IN ('O','A'))
  ) THEN
    RAISE EXCEPTION 'Candidate registry admission and retirement guards are not installed' USING ERRCODE='23514';
  END IF;
END $$;
