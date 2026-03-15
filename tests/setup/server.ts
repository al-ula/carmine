import { spawn, ChildProcess, execSync } from 'child_process';
import { mkdtemp, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

let serverProcess: ChildProcess | null = null;
let tempDir: string | null = null;

const PORT = 13000;
const HOST = '127.0.0.1';
const BASE_URL = `http://${HOST}:${PORT}`;

export function getBaseUrl(): string {
  return BASE_URL;
}

async function waitForServer(url: string, maxAttempts = 30): Promise<void> {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const response = await fetch(`${url}/health`);
      if (response.ok && (await response.text()) === 'OK') {
        return;
      }
    } catch {
      // Server not ready yet
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error('Server failed to start within timeout');
}

function killProcessOnPort(port: number): void {
  try {
    execSync(`fuser -k ${port}/tcp 2>/dev/null || true`, { stdio: 'ignore' });
  } catch {
    // Ignore errors
  }
}

export async function setup(): Promise<void> {
  const projectRoot = join(import.meta.dirname, '..', '..');

  killProcessOnPort(PORT);
  await new Promise((resolve) => setTimeout(resolve, 500));

  tempDir = await mkdtemp(join(tmpdir(), 'carmine-test-'));

  console.log('Building server...');
  const buildResult = await new Promise<{ code: number }>((resolve, reject) => {
    const build = spawn('cargo', ['build', '--release'], {
      cwd: projectRoot,
      stdio: 'inherit',
    });
    build.on('close', (code) => resolve({ code: code ?? 1 }));
    build.on('error', reject);
  });

  if (buildResult.code !== 0) {
    throw new Error('Failed to build server');
  }

  console.log('Starting server...');
  serverProcess = spawn(
    './target/release/carmine',
    ['--bind', `${HOST}:${PORT}`, '--data-dir', tempDir, '--no-config'],
    {
      cwd: projectRoot,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        CARMINE_DURABILITY: 'none',
      },
    }
  );

  serverProcess.stderr?.on('data', (data) => {
    process.stderr.write(data);
  });

  serverProcess.on('error', (err) => {
    console.error('Server process error:', err);
  });

  await waitForServer(BASE_URL);
  console.log('Server ready');
}

export async function teardown(): Promise<void> {
  if (serverProcess) {
    console.log('Stopping server...');
    serverProcess.kill('SIGTERM');
    await new Promise<void>((resolve) => {
      serverProcess!.on('close', () => resolve());
      setTimeout(() => {
        if (serverProcess) {
          serverProcess.kill('SIGKILL');
        }
        resolve();
      }, 5000);
    });
    serverProcess = null;
  }

  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }

  killProcessOnPort(PORT);
}
