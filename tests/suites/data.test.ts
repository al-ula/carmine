import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { client } from '../lib/client.js';

describe('Data operations', () => {
  const testCabinet = `data-test-${Date.now()}`;
  const stringShelf = 'string-shelf';
  const intKeyShelf = 'int-key-shelf';
  const numberKeyShelf = 'number-key-shelf';
  const intValShelf = 'int-val-shelf';
  const objectValShelf = 'object-val-shelf';

  beforeAll(async () => {
    console.log('Creating cabinet:', testCabinet);
    const cabinetResult = await client.createCabinet(testCabinet);
    console.log('Cabinet result:', JSON.stringify(cabinetResult, null, 2));
    
    console.log('Creating shelves...');
    const shelfResults = await Promise.all([
      client.createShelf(testCabinet, stringShelf, 'String', 'String'),
      client.createShelf(testCabinet, intKeyShelf, 'Int', 'String'),
      client.createShelf(testCabinet, numberKeyShelf, 'Number', 'String'),
      client.createShelf(testCabinet, intValShelf, 'String', 'Int'),
      client.createShelf(testCabinet, objectValShelf, 'String', 'Object'),
    ]);
    console.log('Shelf results:', JSON.stringify(shelfResults, null, 2));
  });

  afterAll(async () => {
    await client.deleteCabinet(testCabinet);
  });

  describe('Set and Get', () => {
    it('sets and gets a string key-value pair', async () => {
      const setResult = await client.set(testCabinet, stringShelf, 'my-key', 'my-value');
      expect(setResult.error).toBeNull();
      expect(setResult.status).toBe(204);

      const getResult = await client.get<string, string>(testCabinet, stringShelf, 'my-key');
      expect(getResult.error).toBeNull();
      expect(getResult.data!.value).toBe('my-value');
    });

    it('overwrites existing key with set', async () => {
      await client.set(testCabinet, stringShelf, 'overwrite-key', 'value1');
      await client.set(testCabinet, stringShelf, 'overwrite-key', 'value2');

      const result = await client.get<string, string>(testCabinet, stringShelf, 'overwrite-key');
      expect(result.data!.value).toBe('value2');
    });

    it('returns null for non-existent key', async () => {
      const result = await client.get<string, string>(testCabinet, stringShelf, 'non-existent-key');
      expect(result.error).toBeNull();
      expect(result.data!.value).toBeNull();
    });
  });

  describe('Put (insert-only)', () => {
    it('inserts new key with put', async () => {
      const result = await client.put(testCabinet, stringShelf, 'put-key', 'put-value');
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);
    });

    it('rejects put on existing key', async () => {
      await client.put(testCabinet, stringShelf, 'put-overwrite', 'value1');
      const result = await client.put(testCabinet, stringShelf, 'put-overwrite', 'value2');
      expect(result.status).not.toBe(204);

      const get = await client.get<string, string>(testCabinet, stringShelf, 'put-overwrite');
      expect(get.data!.value).toBe('value1');
    });
  });

  describe('Delete', () => {
    it('deletes a key', async () => {
      await client.set(testCabinet, stringShelf, 'delete-key', 'value');

      const deleteResult = await client.delete(testCabinet, stringShelf, 'delete-key');
      expect(deleteResult.error).toBeNull();
      expect(deleteResult.status).toBe(204);

      const getResult = await client.get<string, string>(testCabinet, stringShelf, 'delete-key');
      expect(getResult.data!.value).toBeNull();
    });

    it('succeeds when deleting non-existent key', async () => {
      const result = await client.delete(testCabinet, stringShelf, 'non-existent-delete');
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);
    });
  });

  describe('All, Keys, Values, Count', () => {
    beforeAll(async () => {
      await client.set(testCabinet, stringShelf, 'all-key1', 'val1');
      await client.set(testCabinet, stringShelf, 'all-key2', 'val2');
      await client.set(testCabinet, stringShelf, 'all-key3', 'val3');
    });

    it('returns all entries', async () => {
      const result = await client.all<string, string>(testCabinet, stringShelf);
      expect(result.error).toBeNull();
      expect(result.data!.entries).toBeInstanceOf(Array);
      expect(result.data!.entries.length).toBeGreaterThanOrEqual(3);

      const keys = result.data!.entries.map((e) => e[0]);
      expect(keys).toContain('all-key1');
      expect(keys).toContain('all-key2');
      expect(keys).toContain('all-key3');
    });

    it('returns all keys', async () => {
      const result = await client.keys<string>(testCabinet, stringShelf);
      expect(result.error).toBeNull();
      expect(result.data!.keys).toBeInstanceOf(Array);
      expect(result.data!.keys).toContain('all-key1');
      expect(result.data!.keys).toContain('all-key2');
      expect(result.data!.keys).toContain('all-key3');
    });

    it('returns all values', async () => {
      const result = await client.values<string>(testCabinet, stringShelf);
      expect(result.error).toBeNull();
      expect(result.data!.values).toBeInstanceOf(Array);
      expect(result.data!.values).toContain('val1');
      expect(result.data!.values).toContain('val2');
      expect(result.data!.values).toContain('val3');
    });

    it('returns count', async () => {
      const result = await client.count(testCabinet, stringShelf);
      expect(result.error).toBeNull();
      expect(result.data!.count).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Range queries', () => {
    beforeAll(async () => {
      await client.set(testCabinet, stringShelf, 'range-a', '1');
      await client.set(testCabinet, stringShelf, 'range-b', '2');
      await client.set(testCabinet, stringShelf, 'range-c', '3');
      await client.set(testCabinet, stringShelf, 'range-d', '4');
    });

    it('returns entries in range', async () => {
      const result = await client.range<string, string>(testCabinet, stringShelf, 'range-a', 'range-c');
      expect(result.error).toBeNull();
      expect(result.data!.entries).toBeInstanceOf(Array);

      const keys = result.data!.entries.map((e) => e[0]);
      expect(keys).toContain('range-a');
      expect(keys).toContain('range-b');
    });
  });

  describe('Exists', () => {
    it('returns true for existing key', async () => {
      await client.set(testCabinet, stringShelf, 'exists-key', 'value');

      const result = await client.exists(testCabinet, stringShelf, 'exists-key');
      expect(result.error).toBeNull();
      expect(result.data!.exists).toBe(true);
    });

    it('returns false for non-existent key', async () => {
      const result = await client.exists(testCabinet, stringShelf, 'non-existent-exists');
      expect(result.error).toBeNull();
      expect(result.data!.exists).toBe(false);
    });
  });

  describe('Batch operations', () => {
    it('batch sets multiple entries', async () => {
      const entries: [string, string][] = [
        ['batch-key1', 'batch-val1'],
        ['batch-key2', 'batch-val2'],
        ['batch-key3', 'batch-val3'],
      ];

      const result = await client.batchSet(testCabinet, stringShelf, entries);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);

      for (const [key, value] of entries) {
        const get = await client.get<string, string>(testCabinet, stringShelf, key);
        expect(get.data!.value).toBe(value);
      }
    });

    it('batch gets multiple values', async () => {
      await client.set(testCabinet, stringShelf, 'batch-get1', 'val1');
      await client.set(testCabinet, stringShelf, 'batch-get2', 'val2');

      const result = await client.batchGet<string, string>(testCabinet, stringShelf, [
        'batch-get1',
        'batch-get2',
        'non-existent',
      ]);
      expect(result.error).toBeNull();
      expect(result.data!.values).toHaveLength(3);
      expect(result.data!.values[0]).toBe('val1');
      expect(result.data!.values[1]).toBe('val2');
      expect(result.data!.values[2]).toBeNull();
    });

    it('batch deletes multiple keys', async () => {
      await client.set(testCabinet, stringShelf, 'batch-del1', 'val1');
      await client.set(testCabinet, stringShelf, 'batch-del2', 'val2');

      const result = await client.batchDelete(testCabinet, stringShelf, ['batch-del1', 'batch-del2']);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);

      const get1 = await client.get<string, string>(testCabinet, stringShelf, 'batch-del1');
      const get2 = await client.get<string, string>(testCabinet, stringShelf, 'batch-del2');
      expect(get1.data!.value).toBeNull();
      expect(get2.data!.value).toBeNull();
    });

    it('batch put inserts entries', async () => {
      const entries: [string, string][] = [
        ['batch-put1', 'put-val1'],
        ['batch-put2', 'put-val2'],
      ];

      const result = await client.batchPut(testCabinet, stringShelf, entries);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);
    });
  });

  describe('Key types', () => {
    it('works with int keys', async () => {
      const result = await client.set<number, string>(testCabinet, intKeyShelf, 42, 'int-value');
      expect(result.error).toBeNull();

      const get = await client.get<number, string>(testCabinet, intKeyShelf, 42);
      expect(get.data!.value).toBe('int-value');
    });

    it('works with number keys', async () => {
      const result = await client.set<number, string>(testCabinet, numberKeyShelf, 3.14, 'number-value');
      expect(result.error).toBeNull();

      const get = await client.get<number, string>(testCabinet, numberKeyShelf, 3.14);
      expect(get.data!.value).toBe('number-value');
    });

    it('returns error for key type mismatch', async () => {
      const result = await client.set<string, string>(testCabinet, intKeyShelf, 'string-key', 'value');
      expect(result.status).toBe(400);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('Key type mismatch'),
      });
    });
  });

  describe('Value types', () => {
    it('works with int values', async () => {
      const result = await client.set<string, number>(testCabinet, intValShelf, 'int-val-key', 123);
      expect(result.error).toBeNull();

      const get = await client.get<string, number>(testCabinet, intValShelf, 'int-val-key');
      expect(get.data!.value).toBe(123);
    });

    it('works with object values', async () => {
      const obj = { name: 'test', count: 42 };
      const result = await client.set<string, object>(testCabinet, objectValShelf, 'obj-key', obj);
      expect(result.error).toBeNull();

      const get = await client.get<string, { name: string; count: number }>(testCabinet, objectValShelf, 'obj-key');
      expect(get.data!.value).toMatchObject(obj);
    });

    it('returns error for value type mismatch', async () => {
      const result = await client.set<string, string>(testCabinet, intValShelf, 'mismatch-key', 'string-value');
      expect(result.status).toBe(400);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('Value type mismatch'),
      });
    });
  });

  describe('Error handling', () => {
    it('returns error for non-existent cabinet', async () => {
      const result = await client.get('non-existent-cabinet', 'some-shelf', 'key');
      expect(result.status).toBe(404);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('not found'),
      });
    });

    it('returns error for non-existent shelf', async () => {
      const result = await client.get(testCabinet, 'non-existent-shelf', 'key');
      expect(result.status).toBe(404);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('not found'),
      });
    });
  });
});
