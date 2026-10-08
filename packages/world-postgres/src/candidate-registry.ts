import { Pool, type PoolClient } from 'pg';

export type CandidateOperationState = 'open' | 'sealed' | 'retired';
export interface CreationCandidate {
  runId: string;
  role: 'session' | 'auxiliary';
  ownerClaimed: boolean;
  lineage: {
    parentRunId: string | null;
    rootRunId: string | null;
    parentTurnId: string | null;
    parentCallId: string | null;
  };
}
export interface CandidateOperationSnapshot {
  operationKey: string;
  parentOperationKey: string | null;
  sourceOperationKey: string | null;
  canonicalRunId: string | null;
  state: CandidateOperationState;
  candidates: CreationCandidate[];
}
export interface TrackedCandidateOperation extends CandidateOperationSnapshot {
  status: 'tracked';
  version: 1;
  operations: CandidateOperationSnapshot[];
  operationKey: string;
  canonicalRunId: string | null;
  state: CandidateOperationState;
  candidates: CreationCandidate[];
}
export type CandidateInventory =
  | TrackedCandidateOperation
  | {
      status: 'inventory-incomplete';
      runId: string;
    };
export class CandidateRegistryConflictError extends Error {
  constructor() {
    super(
      'Candidate operation identity does not match the expected operation.'
    );
    this.name = 'CandidateRegistryConflictError';
  }
}

interface OperationRow {
  protocol_version: number;
  parent_operation_key: string | null;
  source_operation_key: string | null;
  operation_key: string;
  canonical_run_id: string | null;
  state: CandidateOperationState;
}
interface CandidateRow {
  operation_key: string;
  run_id: string;
  role: 'session' | 'auxiliary';
  owner_claimed: boolean;
  lineage: CreationCandidate['lineage'];
}

/**
 * Provider-owned creation inventory. Associations and tombstones outlive run
 * payloads. The caller must separately authorize access to the supplied run.
 * seal() closes admission/ownership before application settlement; retire()
 * hard-fences payload writes only after settlement is durably acknowledged.
 */
export function createCandidateRegistry(config: {
  pool?: Pool;
  connectionString?: string;
}) {
  const pool =
    config.pool ?? new Pool({ connectionString: config.connectionString });

  async function read(
    client: PoolClient,
    runId: string,
    expectedOperationKey?: string,
    target?: 'sealed' | 'retired'
  ): Promise<CandidateInventory> {
    const owner = await client.query<{ operation_key: string }>(
      'SELECT operation_key FROM workflow.creation_candidates WHERE run_id=$1',
      [runId]
    );
    const key = owner.rows[0]?.operation_key;
    if (key === undefined) return { status: 'inventory-incomplete', runId };
    if (expectedOperationKey !== undefined && key !== expectedOperationKey) {
      throw new CandidateRegistryConflictError();
    }
    await client.query('SELECT workflow.lock_creation_family($1)', [key]);
    const result = await client.query<OperationRow>(
      `WITH RECURSIVE owned AS (
         SELECT operation_key FROM workflow.creation_operations WHERE operation_key=$1
         UNION ALL SELECT o.operation_key FROM workflow.creation_operations o
           JOIN owned p ON o.parent_operation_key=p.operation_key
       ) SELECT o.* FROM workflow.creation_operations o JOIN owned USING(operation_key)
         ORDER BY o.operation_key FOR UPDATE OF o`,
      [key]
    );
    if (
      result.rows.length === 0 ||
      result.rows.some((row) => row.protocol_version !== 1)
    ) {
      return { status: 'inventory-incomplete', runId };
    }
    const keys = result.rows.map((row) => row.operation_key);
    if (
      target === 'retired' &&
      result.rows.some((row) => row.state === 'open')
    ) {
      throw new Error(
        'Seal every owned candidate operation before retiring its payloads.'
      );
    }
    if (target === 'sealed') {
      await client.query(
        `UPDATE workflow.creation_operations SET state='sealed',sealed_at=now()
        WHERE operation_key=ANY($1::text[]) AND state='open'`,
        [keys]
      );
      for (const row of result.rows)
        if (row.state === 'open') row.state = 'sealed';
    }
    if (target === 'retired') {
      await client.query(
        `UPDATE workflow.creation_operations SET state='retired',retired_at=now()
        WHERE operation_key=ANY($1::text[]) AND state='sealed'`,
        [keys]
      );
      for (const row of result.rows) row.state = 'retired';
    }
    const candidates = await client.query<CandidateRow>(
      `SELECT operation_key,run_id,role,owner_claimed,lineage FROM workflow.creation_candidates
       WHERE operation_key=ANY($1::text[]) ORDER BY run_id`,
      [keys]
    );
    const operations = result.rows.map((row) => ({
      operationKey: row.operation_key,
      parentOperationKey: row.parent_operation_key,
      sourceOperationKey: row.source_operation_key,
      canonicalRunId: row.canonical_run_id,
      state: row.state,
      candidates: candidates.rows
        .filter((candidate) => candidate.operation_key === row.operation_key)
        .map((candidate) => ({
          runId: candidate.run_id,
          role: candidate.role,
          ownerClaimed: candidate.owner_claimed,
          lineage: candidate.lineage,
        })),
    }));
    const root = operations.find((operation) => operation.operationKey === key);
    if (root === undefined) return { status: 'inventory-incomplete', runId };
    return { status: 'tracked', version: 1, ...root, operations };
  }

  async function transact(
    input: { runId: string; expectedOperationKey?: string },
    target?: 'sealed' | 'retired'
  ): Promise<CandidateInventory> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT workflow.assert_creation_registry_ready()');
      const result = await read(
        client,
        input.runId,
        input.expectedOperationKey,
        target
      );
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  return {
    async assertReady() {
      await pool.query('SELECT workflow.assert_creation_registry_ready()');
    },
    inspect: (input: { runId: string }) => transact(input),
    seal: (input: { runId: string; expectedOperationKey: string }) =>
      transact(input, 'sealed'),
    retire: (input: { runId: string; expectedOperationKey: string }) =>
      transact(input, 'retired'),
    async close() {
      if (!config.pool) await pool.end();
    },
  };
}
