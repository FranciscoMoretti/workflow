import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, describe, expect, test } from 'vitest';
import {
  CandidateRegistryConflictError,
  createCandidateRegistry,
} from '../src/candidate-registry.js';
import { createClient } from '../src/drizzle/index.js';
import { createEventsStorage } from '../src/storage.js';

const connectionString = process.env.WORKFLOW_POSTGRES_TEST_URL;
if (!connectionString)
  throw new Error(
    'Set WORKFLOW_POSTGRES_TEST_URL to an isolated migrated database.'
  );
const pool = new Pool({ connectionString });
const registry = createCandidateRegistry({ pool });
afterAll(() => pool.end());

function envelope(operationKey: string, role = 'session') {
  return {
    '$eve.creation.key': operationKey,
    '$eve.creation.intent': 'a'.repeat(64),
    '$eve.creation.role': role,
    '$eve.creation.claim_token': `claim:${operationKey}`,
  };
}
const makeId = () => `wrun_${randomUUID()}`;
async function insertRun(
  runId: string,
  attrs: Record<string, string>,
  name = 'workflow//eve//workflowEntry'
) {
  return pool.query(
    `INSERT INTO workflow.workflow_runs(id,name,deployment_id,status,input,attributes)
    VALUES($1,$2,'test','pending',$3,$4)`,
    [runId, name, JSON.stringify(['private input']), attrs]
  );
}
async function enqueue(runId: string, attrs?: Record<string, string>) {
  return pool.query(
    `SELECT graphile_worker.add_job('candidate-test', $1::json)`,
    [
      JSON.stringify({
        id: 'workflow//eve//workflowEntry',
        runId,
        messageId: randomUUID(),
        data: 'cHJpdmF0ZSBxdWV1ZSBpbnB1dA==',
        initialRun: true,
        creationAttributes: attrs,
      }),
    ]
  );
}
async function claim(runId: string, operationKey: string) {
  return pool.query(
    `INSERT INTO workflow.workflow_hooks(run_id,hook_id,token,owner_id,project_id,environment)
    VALUES($1,$2,$3,'','','')`,
    [runId, randomUUID(), `claim:${operationKey}`]
  );
}

describe('durable candidate admission and retirement', () => {
  test('retains immutable native attribution through queue-first admission and payload purge', async () => {
    const root = makeId(),
      child = makeId(),
      rootKey = randomUUID(),
      childKey = randomUUID();
    await insertRun(root, envelope(rootKey));
    await claim(root, rootKey);
    const attrs = {
      ...envelope(childKey),
      '$eve.parent': root,
      '$eve.root': root,
      '$eve.parent_turn': 'turn_parent',
      '$eve.parent_call': 'call_child',
    };
    await enqueue(child, attrs);
    await expect(
      insertRun(child, { ...attrs, '$eve.parent_turn': 'forged' })
    ).rejects.toThrow(/lineage/);
    await insertRun(child, attrs);
    await claim(child, childKey);
    await expect(
      pool.query(
        `UPDATE workflow.workflow_runs SET attributes=attributes || '{"$eve.parent_turn":"forged"}'::jsonb WHERE id=$1`,
        [child]
      )
    ).rejects.toThrow(/immutable/);
    await expect(
      pool.query(
        `UPDATE workflow.creation_candidates SET lineage='{}'::jsonb WHERE run_id=$1`,
        [child]
      )
    ).rejects.toThrow(/immutable/);
    await registry.seal({ runId: root, expectedOperationKey: rootKey });
    await registry.retire({ runId: root, expectedOperationKey: rootKey });
    await pool.query('DELETE FROM workflow.workflow_hooks WHERE run_id=$1', [
      child,
    ]);
    await pool.query('DELETE FROM workflow.workflow_runs WHERE id=$1', [child]);
    expect(await registry.inspect({ runId: child })).toMatchObject({
      status: 'tracked',
      candidates: [
        {
          runId: child,
          ownerClaimed: true,
          lineage: {
            parentRunId: root,
            rootRunId: root,
            parentTurnId: 'turn_parent',
            parentCallId: 'call_child',
          },
        },
      ],
    });
  });
  test('owned descendants close with the family while source-only copies remain independent', async () => {
    const root = makeId(),
      child = makeId(),
      copy = makeId();
    const rootKey = randomUUID(),
      childKey = randomUUID(),
      copyKey = randomUUID();
    await insertRun(root, envelope(rootKey));
    await claim(root, rootKey);
    await insertRun(child, { ...envelope(childKey), '$eve.parent': root });
    await claim(child, childKey);
    await insertRun(copy, {
      ...envelope(copyKey),
      '$eve.creation.source_run': root,
    });
    await claim(copy, copyKey);
    const sealed = await registry.seal({
      runId: root,
      expectedOperationKey: rootKey,
    });
    expect(sealed).toMatchObject({ status: 'tracked', state: 'sealed' });
    if (sealed.status !== 'tracked') throw new Error('Missing registry');
    expect(
      sealed.operations.map((operation) => operation.operationKey).sort()
    ).toEqual([rootKey, childKey].sort());
    expect(
      sealed.operations.map((operation) => operation.canonicalRunId).sort()
    ).toEqual([root, child].sort());
    expect(await registry.inspect({ runId: copy })).toMatchObject({
      state: 'open',
      sourceOperationKey: rootKey,
      parentOperationKey: null,
    });
    await expect(
      insertRun(makeId(), { ...envelope(randomUUID()), '$eve.parent': child })
    ).rejects.toThrow(/sealed/);
    await registry.retire({ runId: root, expectedOperationKey: rootKey });
    await expect(
      pool.query(
        "UPDATE workflow.workflow_runs SET status='running' WHERE id=$1",
        [child]
      )
    ).rejects.toThrow(/retired/);
    await pool.query(
      "UPDATE workflow.workflow_runs SET status='running' WHERE id=$1",
      [copy]
    );
  });
  test('racing native alias claims produce one owner and ordinary hook conflicts, including after hook deletion', async () => {
    const key = randomUUID(),
      candidates = Array.from({ length: 12 }, makeId);
    const events = createEventsStorage(createClient(pool));
    await Promise.all(
      candidates.map(async (runId) => {
        await insertRun(runId, envelope(key));
        await pool.query(
          'UPDATE workflow.workflow_runs SET spec_version=2 WHERE id=$1',
          [runId]
        );
      })
    );
    const results = await Promise.all(
      candidates.map((runId) =>
        events.create(runId, {
          eventType: 'hook_created',
          correlationId: `hook_${randomUUID()}`,
          eventData: { token: `claim:${key}` },
        })
      )
    );
    expect(
      results.filter((result) => result.event.eventType === 'hook_created')
    ).toHaveLength(1);
    expect(
      results.filter((result) => result.event.eventType === 'hook_conflict')
    ).toHaveLength(11);
    const inventory = await registry.inspect({ runId: candidates[0] });
    expect(inventory.status).toBe('tracked');
    if (inventory.status !== 'tracked') throw new Error('Missing registry');
    expect(
      inventory.candidates.filter((candidate) => candidate.ownerClaimed)
    ).toHaveLength(1);
    await pool.query('DELETE FROM workflow.workflow_hooks WHERE token=$1', [
      `claim:${key}`,
    ]);
    const retry = makeId();
    await insertRun(retry, envelope(key));
    await pool.query(
      'UPDATE workflow.workflow_runs SET spec_version=2 WHERE id=$1',
      [retry]
    );
    expect(
      (
        await events.create(retry, {
          eventType: 'hook_created',
          correlationId: `hook_${randomUUID()}`,
          eventData: { token: `claim:${key}` },
        })
      ).event.eventType
    ).toBe('hook_conflict');
  });
  test('queue-first association exists before any workflow run and survives payload deletion', async () => {
    const runId = makeId(),
      key = randomUUID();
    await enqueue(runId, envelope(key));
    expect(
      (
        await pool.query('SELECT id FROM workflow.workflow_runs WHERE id=$1', [
          runId,
        ])
      ).rowCount
    ).toBe(0);
    expect(await registry.inspect({ runId })).toMatchObject({
      status: 'tracked',
      operationKey: key,
      candidates: [{ runId, ownerClaimed: false }],
    });
    await insertRun(runId, envelope(key));
    await claim(runId, key);
    await registry.seal({ runId, expectedOperationKey: key });
    await registry.retire({ runId, expectedOperationKey: key });
    await pool.query('DELETE FROM workflow.workflow_hooks WHERE run_id=$1', [
      runId,
    ]);
    await pool.query('DELETE FROM workflow.workflow_runs WHERE id=$1', [runId]);
    expect(await registry.inspect({ runId })).toMatchObject({
      status: 'tracked',
      canonicalRunId: runId,
      state: 'retired',
    });
    await expect(insertRun(runId, envelope(key))).rejects.toThrow(/sealed/);
    await expect(enqueue(runId, envelope(key))).rejects.toThrow(/sealed/);
  });
  test('event and queue payload transactions roll back association together', async () => {
    for (const kind of ['event', 'queue']) {
      const client = await pool.connect(),
        runId = makeId(),
        key = randomUUID();
      try {
        await client.query('BEGIN');
        if (kind === 'event')
          await client.query(
            `INSERT INTO workflow.workflow_runs(id,name,deployment_id,status,attributes)
          VALUES($1,'workflow//eve//workflowEntry','test','pending',$2)`,
            [runId, envelope(key)]
          );
        else
          await client.query(
            `SELECT graphile_worker.add_job('candidate-test',$1::json)`,
            [
              JSON.stringify({
                runId,
                initialRun: true,
                creationAttributes: envelope(key),
              }),
            ]
          );
        await client.query('ROLLBACK');
      } finally {
        client.release();
      }
      expect(await registry.inspect({ runId })).toEqual({
        status: 'inventory-incomplete',
        runId,
      });
    }
  });
  test('seal stabilizes candidate and owner snapshot while permitting existing settlement writes', async () => {
    const runId = makeId(),
      loser = makeId(),
      key = randomUUID();
    await insertRun(runId, envelope(key));
    await insertRun(loser, envelope(key));
    await claim(runId, key);
    expect(
      await registry.seal({ runId, expectedOperationKey: key })
    ).toMatchObject({ state: 'sealed', canonicalRunId: runId });
    await expect(claim(loser, key)).rejects.toThrow(/sealed/);
    await expect(insertRun(makeId(), envelope(key))).rejects.toThrow(/sealed/);
    await enqueue(runId, envelope(key));
    await pool.query(
      `INSERT INTO workflow.workflow_stream_chunks(id,stream_id,run_id,data,eof) VALUES($1,$2,$2,$3,false)`,
      [randomUUID(), runId, Buffer.from('settlement')]
    );
    await registry.retire({ runId, expectedOperationKey: key });
    await expect(
      pool.query(
        `INSERT INTO workflow.workflow_stream_chunks(id,stream_id,run_id,data,eof) VALUES($1,$2,$2,$3,false)`,
        [randomUUID(), runId, Buffer.from('late')]
      )
    ).rejects.toThrow(/retired/);
  });
  test('concurrent admissions retain every candidate and exactly one canonical claim', async () => {
    const key = randomUUID(),
      ids = Array.from({ length: 24 }, makeId);
    await Promise.all(ids.map((id) => enqueue(id, envelope(key))));
    await Promise.all(ids.map((id) => insertRun(id, envelope(key))));
    const outcomes = await Promise.allSettled(ids.map((id) => claim(id, key)));
    expect(
      outcomes.filter((outcome) => outcome.status === 'fulfilled')
    ).toHaveLength(1);
    const snapshot = await registry.seal({
      runId: ids[0],
      expectedOperationKey: key,
    });
    expect(snapshot.status).toBe('tracked');
    if (snapshot.status !== 'tracked') throw new Error('Missing registry');
    expect(snapshot.candidates).toHaveLength(24);
    expect(
      snapshot.candidates.filter((candidate) => candidate.ownerClaimed)
    ).toHaveLength(1);
  });
  test('identity, intent, and role cannot change; unknown inventory is not fabricated', async () => {
    const runId = makeId(),
      key = randomUUID();
    await enqueue(runId, envelope(key));
    await expect(
      enqueue(makeId(), {
        ...envelope(key),
        '$eve.creation.intent': 'b'.repeat(64),
      })
    ).rejects.toThrow(/intent mismatch/);
    await expect(enqueue(runId, envelope(key, 'auxiliary'))).rejects.toThrow(
      /immutable/
    );
    await expect(
      registry.seal({ runId, expectedOperationKey: 'wrong' })
    ).rejects.toBeInstanceOf(CandidateRegistryConflictError);
    expect(await registry.inspect({ runId: makeId() })).toMatchObject({
      status: 'inventory-incomplete',
    });
  });
  test('tracked stream identity remains fenced after payload purge, including null run IDs', async () => {
    const runId = makeId(),
      key = randomUUID(),
      streamId = randomUUID();
    await insertRun(runId, envelope(key));
    await pool.query(
      `INSERT INTO workflow.workflow_stream_chunks(id,stream_id,run_id,data,eof) VALUES($1,$2,$3,$4,false)`,
      [randomUUID(), streamId, runId, Buffer.from('original')]
    );
    await registry.seal({ runId, expectedOperationKey: key });
    await registry.retire({ runId, expectedOperationKey: key });
    await pool.query(
      'DELETE FROM workflow.workflow_stream_chunks WHERE run_id=$1',
      [runId]
    );
    await expect(
      pool.query(
        `INSERT INTO workflow.workflow_stream_chunks(id,stream_id,run_id,data,eof) VALUES($1,$2,NULL,$3,false)`,
        [randomUUID(), streamId, Buffer.from('late')]
      )
    ).rejects.toThrow(/immutable/);
  });
  test('generic descendants inherit tracked parent admission and cannot escape its fence', async () => {
    const runId = makeId(),
      child = makeId(),
      key = randomUUID();
    await insertRun(runId, envelope(key));
    await insertRun(
      child,
      { $parentRunId: runId },
      'workflow//application//helper'
    );
    expect(await registry.inspect({ runId: child })).toMatchObject({
      status: 'tracked',
      operationKey: key,
      candidates: expect.arrayContaining([
        expect.objectContaining({
          runId: child,
          role: 'auxiliary',
          ownerClaimed: false,
        }),
      ]),
    });
    await registry.seal({ runId, expectedOperationKey: key });
    await expect(
      insertRun(
        makeId(),
        { $parentRunId: runId },
        'workflow//application//helper'
      )
    ).rejects.toThrow(/sealed/);
  });
});
