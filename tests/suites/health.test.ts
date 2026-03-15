import { describe, it, expect } from 'vitest';
import { client } from '../lib/client.js';

describe('Health check', () => {
  it('returns OK', async () => {
    const result = await client.health();
    expect(result).toBe('OK');
  });
});
