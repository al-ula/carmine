import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { client } from '../lib/client.js';
import type { CabinetMeta, ShelfMeta } from '../lib/types.js';

describe('System API', () => {
  const testCabinet = `test-cabinet-${Date.now()}`;
  let createdCabinet: CabinetMeta | null = null;

  afterAll(async () => {
    const cabinets = await client.listCabinets();
    if (cabinets.data) {
      for (const c of cabinets.data) {
        if (c.name.startsWith('test-')) {
          await client.deleteCabinet(c.name);
        }
      }
    }
  });

  describe('Cabinet operations', () => {
    it('creates a cabinet', async () => {
      const result = await client.createCabinet(testCabinet);
      expect(result.error).toBeNull();
      expect(result.status).toBe(200);
      expect(result.data).toMatchObject({
        name: testCabinet,
        shelves: [],
      });
      expect(result.data!.id).toBeGreaterThan(0);
      expect(result.data!.path).toContain('cabinet_');
      createdCabinet = result.data;
    });

    it('returns error for duplicate cabinet', async () => {
      const result = await client.createCabinet(testCabinet);
      expect(result.status).toBe(409);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('already exists'),
      });
    });

    it('lists cabinets', async () => {
      const result = await client.listCabinets();
      expect(result.error).toBeNull();
      expect(result.status).toBe(200);
      expect(result.data).toBeInstanceOf(Array);
      expect(result.data!.some((c) => c.name === testCabinet)).toBe(true);
    });

    it('gets a cabinet by name', async () => {
      const result = await client.getCabinet(testCabinet);
      expect(result.error).toBeNull();
      expect(result.status).toBe(200);
      expect(result.data).toMatchObject({
        name: testCabinet,
        id: createdCabinet!.id,
      });
    });

    it('returns error for non-existent cabinet', async () => {
      const result = await client.getCabinet('non-existent-cabinet');
      expect(result.status).toBe(404);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('not found'),
      });
    });
  });

  describe('Shelf operations', () => {
    const testShelf = 'test-shelf';
    let createdShelf: ShelfMeta | null = null;

    it('creates a shelf', async () => {
      const result = await client.createShelf(testCabinet, testShelf, 'String', 'String');
      expect(result.error).toBeNull();
      expect(result.status).toBe(200);
      expect(result.data).toMatchObject({
        name: testShelf,
        key_type: 'String',
        value_type: 'String',
      });
      createdShelf = result.data;
    });

    it('returns error for duplicate shelf', async () => {
      const result = await client.createShelf(testCabinet, testShelf, 'String', 'String');
      expect(result.status).toBe(409);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('already exists'),
      });
    });

    it('lists shelves', async () => {
      const result = await client.listShelves(testCabinet);
      expect(result.error).toBeNull();
      expect(result.status).toBe(200);
      expect(result.data).toBeInstanceOf(Array);
      expect(result.data!.some((s) => s.name === testShelf)).toBe(true);
    });

    it('returns error when creating shelf in non-existent cabinet', async () => {
      const result = await client.createShelf('non-existent-cabinet', 'some-shelf', 'String', 'String');
      expect(result.status).toBe(404);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('not found'),
      });
    });

    it('creates shelves with different key types', async () => {
      const intShelf = await client.createShelf(testCabinet, 'int-key-shelf', 'Int', 'String');
      expect(intShelf.error).toBeNull();
      expect(intShelf.data!.key_type).toBe('Int');

      const numShelf = await client.createShelf(testCabinet, 'number-key-shelf', 'Number', 'String');
      expect(numShelf.error).toBeNull();
      expect(numShelf.data!.key_type).toBe('Number');
    });

    it('creates shelves with different value types', async () => {
      const intValShelf = await client.createShelf(testCabinet, 'int-val-shelf', 'String', 'Int');
      expect(intValShelf.error).toBeNull();
      expect(intValShelf.data!.value_type).toBe('Int');

      const objShelf = await client.createShelf(testCabinet, 'object-val-shelf', 'String', 'Object');
      expect(objShelf.error).toBeNull();
      expect(objShelf.data!.value_type).toBe('Object');
    });

    it('deletes a shelf', async () => {
      const shelfToDelete = 'shelf-to-delete';
      await client.createShelf(testCabinet, shelfToDelete, 'String', 'String');

      const result = await client.deleteShelf(testCabinet, shelfToDelete);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);

      const shelves = await client.listShelves(testCabinet);
      expect(shelves.data!.some((s) => s.name === shelfToDelete)).toBe(false);
    });

    it('returns error when deleting non-existent shelf', async () => {
      const result = await client.deleteShelf(testCabinet, 'non-existent-shelf');
      expect(result.status).toBe(404);
      expect(result.error).toMatchObject({
        error: expect.stringContaining('not found'),
      });
    });
  });

  describe('Cabinet cleanup', () => {
    const cleanTestCabinet = `clean-test-${Date.now()}`;
    const cleanTestShelf = 'clean-shelf';

    beforeAll(async () => {
      await client.createCabinet(cleanTestCabinet);
      await client.createShelf(cleanTestCabinet, cleanTestShelf, 'String', 'String');
      await client.set(cleanTestCabinet, cleanTestShelf, 'key1', 'value1');
      await client.set(cleanTestCabinet, cleanTestShelf, 'key2', 'value2');
    });

    it('cleans a cabinet', async () => {
      const beforeClean = await client.count(cleanTestCabinet, cleanTestShelf);
      expect(beforeClean.data!.count).toBe(2);

      const result = await client.cleanCabinet(cleanTestCabinet);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);

      const afterClean = await client.count(cleanTestCabinet, cleanTestShelf);
      expect(afterClean.data!.count).toBe(0);
    });

    it('returns error when cleaning non-existent cabinet', async () => {
      const result = await client.cleanCabinet('non-existent-cabinet');
      expect(result.status).toBe(404);
    });
  });

  describe('Delete cabinet', () => {
    it('deletes a cabinet', async () => {
      const cabinetToDelete = `delete-test-${Date.now()}`;
      await client.createCabinet(cabinetToDelete);

      const result = await client.deleteCabinet(cabinetToDelete);
      expect(result.error).toBeNull();
      expect(result.status).toBe(204);

      const getResult = await client.getCabinet(cabinetToDelete);
      expect(getResult.status).toBe(404);
    });
  });
});
