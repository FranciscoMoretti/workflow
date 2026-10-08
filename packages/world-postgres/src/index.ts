import type { Storage, World } from '@workflow/world';
import { mintedSpecVersion, reenqueueActiveRuns } from '@workflow/world';
import { Pool } from 'pg';
import { createCandidateRegistry } from './candidate-registry.js';
import type { PostgresWorldConfig } from './config.js';
import { createClient, type Drizzle } from './drizzle/index.js';
import { createQueue } from './queue.js';
import {
  createRunStatusListener,
  type RunStatusListener,
} from './run-status.js';
import {
  createEventsStorage,
  createHooksStorage,
  createRunsStorage,
  createStepsStorage,
} from './storage.js';
import { createStreamer } from './streamer.js';

function createStorage(
  drizzle: Drizzle,
  runStatusListener: RunStatusListener
): Storage {
  return {
    runs: createRunsStorage(drizzle, runStatusListener),
    events: createEventsStorage(drizzle),
    hooks: createHooksStorage(drizzle),
    steps: createStepsStorage(drizzle),
  };
}

function getDefaultMaxPoolSize(): number | undefined {
  const parsed = parseInt(
    process.env.WORKFLOW_POSTGRES_MAX_POOL_SIZE || '',
    10
  );

  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function getDefaultConnectionString(): string {
  return (
    process.env.WORKFLOW_POSTGRES_URL ||
    process.env.DATABASE_URL ||
    'postgres://world:world@localhost:5432/world'
  );
}

export function createWorld(
  config: PostgresWorldConfig = {
    connectionString: getDefaultConnectionString(),
    jobPrefix: process.env.WORKFLOW_POSTGRES_JOB_PREFIX,
    queueConcurrency:
      parseInt(process.env.WORKFLOW_POSTGRES_WORKER_CONCURRENCY || '50', 10) ||
      50,
    applicationManagedShutdown:
      process.env.WORKFLOW_POSTGRES_APPLICATION_MANAGED_SHUTDOWN === '1',
  }
): World & {
  start(): Promise<void>;
  candidateRegistry: ReturnType<typeof createCandidateRegistry>;
} {
  const maxPoolSize = config.maxPoolSize ?? getDefaultMaxPoolSize();
  const pool =
    config.pool ||
    new Pool({
      connectionString: config.connectionString || getDefaultConnectionString(),
      ...(maxPoolSize !== undefined ? { max: maxPoolSize } : {}),
    });

  const candidateRegistry = createCandidateRegistry({ pool });
  const drizzle = createClient(pool);
  const queue = createQueue(config, pool);
  // Opens its `LISTEN` connection lazily, on the first `waitForTerminalStatus`
  // call, so a deployment that never awaits a run never pays for it.
  const runStatusListener = createRunStatusListener(pool);
  const storage = createStorage(drizzle, runStatusListener);
  const streamer = createStreamer(pool, drizzle);

  return {
    specVersion: mintedSpecVersion(),
    capabilities: {
      hookRetention: { active: true },
    },
    ...storage,
    ...streamer,
    ...queue,
    candidateRegistry,
    events: {
      ...storage.events,
      async create(...args: Parameters<Storage['events']['create']>) {
        await candidateRegistry.assertReady();
        return storage.events.create(...args);
      },
    },
    async queue(...args) {
      await candidateRegistry.assertReady();
      return queue.queue(...args);
    },
    streams: {
      ...streamer.streams,
      async write(...args) {
        await candidateRegistry.assertReady();
        return streamer.streams.write(...args);
      },
      async writeMulti(...args) {
        await candidateRegistry.assertReady();
        return streamer.streams.writeMulti!(...args);
      },
      async close(...args) {
        await candidateRegistry.assertReady();
        return streamer.streams.close(...args);
      },
    },
    ...(config.streamFlushIntervalMs !== undefined && {
      streamFlushIntervalMs: config.streamFlushIntervalMs,
    }),
    async start() {
      await candidateRegistry.assertReady();
      await queue.start();
      await reenqueueActiveRuns(
        storage.runs,
        queue.queue,
        'world-postgres',
        config.namespace
      );
    },
    async close() {
      await queue.close();
      await streamer.close();
      await runStatusListener.close();
      if (pool !== config.pool) {
        await pool.end();
      }
    },
  };
}

export type {
  CandidateInventory,
  CandidateOperationSnapshot,
  CandidateOperationState,
  CreationCandidate,
  TrackedCandidateOperation,
} from './candidate-registry.js';
export {
  CandidateRegistryConflictError,
  createCandidateRegistry,
} from './candidate-registry.js';
// Re-export schema for users who want to extend or inspect the database schema
export type { PostgresWorldConfig } from './config.js';
export * from './drizzle/schema.js';
