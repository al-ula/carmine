import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    timeout: 30000,
    testTimeout: 10000,
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    fileParallelism: false,
    globalSetup: ['./setup/server.ts'],
  },
});
