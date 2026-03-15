import { getBaseUrl } from '../setup/server.js';
import type {
  CabinetMeta,
  ShelfMeta,
  ApiError,
  GetResponse,
  AllResponse,
  KeysResponse,
  ValuesResponse,
  ExistsResponse,
  CountResponse,
  BatchGetResponse,
  KeyType,
  ValueType,
} from './types.js';

export class ApiClient {
  private baseUrl: string;

  constructor() {
    this.baseUrl = getBaseUrl();
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown
  ): Promise<{ data: T | null; error: ApiError | null; status: number }> {
    const url = `${this.baseUrl}${path}`;
    const options: RequestInit = {
      method,
      headers: {
        'Content-Type': 'application/json',
      },
    };

    if (body !== undefined) {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);
    const status = response.status;

    if (response.status === 204) {
      return { data: null, error: null, status };
    }

    const text = await response.text();
    if (!text) {
      return { data: null, error: null, status };
    }

    const json = JSON.parse(text);

    if (!response.ok) {
      return { data: null, error: json as ApiError, status };
    }

    return { data: json as T, error: null, status };
  }

  async health(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/health`);
    return response.text();
  }

  async createCabinet(name: string): Promise<{ data: CabinetMeta | null; error: ApiError | null; status: number }> {
    return this.request<CabinetMeta>('POST', '/system/cabinets', { name });
  }

  async listCabinets(): Promise<{ data: CabinetMeta[] | null; error: ApiError | null; status: number }> {
    return this.request<CabinetMeta[]>('GET', '/system/cabinets');
  }

  async getCabinet(name: string): Promise<{ data: CabinetMeta | null; error: ApiError | null; status: number }> {
    return this.request<CabinetMeta>('GET', `/system/cabinets/${encodeURIComponent(name)}`);
  }

  async deleteCabinet(name: string): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('DELETE', `/system/cabinets/${encodeURIComponent(name)}`);
  }

  async cleanCabinet(name: string): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/system/cabinets/${encodeURIComponent(name)}/clean`);
  }

  async createShelf(
    cabinet: string,
    name: string,
    keyType: KeyType,
    valueType: ValueType
  ): Promise<{ data: ShelfMeta | null; error: ApiError | null; status: number }> {
    return this.request<ShelfMeta>('POST', `/system/cabinets/${encodeURIComponent(cabinet)}/shelves`, {
      name,
      key_type: keyType,
      value_type: valueType,
    });
  }

  async listShelves(cabinet: string): Promise<{ data: ShelfMeta[] | null; error: ApiError | null; status: number }> {
    return this.request<ShelfMeta[]>('GET', `/system/cabinets/${encodeURIComponent(cabinet)}/shelves`);
  }

  async deleteShelf(
    cabinet: string,
    shelf: string
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('DELETE', `/system/cabinets/${encodeURIComponent(cabinet)}/shelves/${encodeURIComponent(shelf)}`);
  }

  async set<K, V>(
    cabinet: string,
    shelf: string,
    key: K,
    value: V
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/set`, {
      key,
      value,
    });
  }

  async put<K, V>(
    cabinet: string,
    shelf: string,
    key: K,
    value: V
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/put`, {
      key,
      value,
    });
  }

  async get<K, V>(
    cabinet: string,
    shelf: string,
    key: K
  ): Promise<{ data: GetResponse<V> | null; error: ApiError | null; status: number }> {
    return this.request<GetResponse<V>>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/get`, {
      key,
    });
  }

  async delete<K>(
    cabinet: string,
    shelf: string,
    key: K
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/delete`, {
      key,
    });
  }

  async all<K, V>(
    cabinet: string,
    shelf: string
  ): Promise<{ data: AllResponse<K, V> | null; error: ApiError | null; status: number }> {
    return this.request<AllResponse<K, V>>('GET', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/all`);
  }

  async keys<K>(
    cabinet: string,
    shelf: string
  ): Promise<{ data: KeysResponse<K> | null; error: ApiError | null; status: number }> {
    return this.request<KeysResponse<K>>('GET', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/keys`);
  }

  async values<V>(
    cabinet: string,
    shelf: string
  ): Promise<{ data: ValuesResponse<V> | null; error: ApiError | null; status: number }> {
    return this.request<ValuesResponse<V>>('GET', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/values`);
  }

  async range<K, V>(
    cabinet: string,
    shelf: string,
    start: K,
    end: K
  ): Promise<{ data: AllResponse<K, V> | null; error: ApiError | null; status: number }> {
    return this.request<AllResponse<K, V>>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/range`, {
      start,
      end,
    });
  }

  async exists<K>(
    cabinet: string,
    shelf: string,
    key: K
  ): Promise<{ data: ExistsResponse | null; error: ApiError | null; status: number }> {
    return this.request<ExistsResponse>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/exists`, {
      key,
    });
  }

  async count(
    cabinet: string,
    shelf: string
  ): Promise<{ data: CountResponse | null; error: ApiError | null; status: number }> {
    return this.request<CountResponse>('GET', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/count`);
  }

  async batchSet<K, V>(
    cabinet: string,
    shelf: string,
    entries: [K, V][]
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/batch/set`, {
      entries,
    });
  }

  async batchPut<K, V>(
    cabinet: string,
    shelf: string,
    entries: [K, V][]
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/batch/put`, {
      entries,
    });
  }

  async batchDelete<K>(
    cabinet: string,
    shelf: string,
    keys: K[]
  ): Promise<{ data: null; error: ApiError | null; status: number }> {
    return this.request<null>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/batch/delete`, {
      keys,
    });
  }

  async batchGet<K, V>(
    cabinet: string,
    shelf: string,
    keys: K[]
  ): Promise<{ data: BatchGetResponse<V> | null; error: ApiError | null; status: number }> {
    return this.request<BatchGetResponse<V>>('POST', `/v1/${encodeURIComponent(cabinet)}/${encodeURIComponent(shelf)}/batch/get`, {
      keys,
    });
  }
}

export const client = new ApiClient();
