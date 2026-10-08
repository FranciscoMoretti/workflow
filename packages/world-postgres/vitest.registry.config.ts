import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/candidate-registry.integration.ts'],
    testTimeout: 60_000,
  },
});
