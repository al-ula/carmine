export interface ShelfMeta {
  name: string;
  key_type: string;
  value_type: string;
}

export interface CabinetMeta {
  id: number;
  name: string;
  path: string;
  shelves: ShelfMeta[];
}

export interface ApiError {
  error: string;
}

export type KeyType = 'String' | 'Int' | 'Number';
export type ValueType = 'String' | 'Int' | 'Number' | 'Object' | 'Byte';

export interface GetResponse<T = unknown> {
  value: T | null;
}

export interface AllResponse<K = unknown, V = unknown> {
  entries: [K, V][];
}

export interface KeysResponse<K = unknown> {
  keys: K[];
}

export interface ValuesResponse<V = unknown> {
  values: V[];
}

export interface ExistsResponse {
  exists: boolean;
}

export interface CountResponse {
  count: number;
}

export interface BatchGetResponse<V = unknown> {
  values: (V | null)[];
}
