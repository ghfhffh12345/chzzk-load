#!/usr/bin/env node

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const PLATFORMS = {
  'win32-x64': {
    pkgName: 'chzzk-load-win32-x64',
    binName: 'chzzk-load.exe',
  },
  'linux-x64': {
    pkgName: 'chzzk-load-linux-x64',
    binName: 'chzzk-load',
  },
  'linux-arm64': {
    pkgName: 'chzzk-load-linux-arm64',
    binName: 'chzzk-load',
  },
  'darwin-x64': {
    pkgName: 'chzzk-load-darwin-x64',
    binName: 'chzzk-load',
  },
  'darwin-arm64': {
    pkgName: 'chzzk-load-darwin-arm64',
    binName: 'chzzk-load',
  },
};

function getPlatformSpec() {
  const key = `${process.platform}-${process.arch}`;
  return PLATFORMS[key] || null;
}

function findBinary() {
  // 1. Environment variable override
  if (process.env.CHZZK_LOAD_BIN) {
    const customPath = process.env.CHZZK_LOAD_BIN;
    if (fs.existsSync(customPath)) {
      return customPath;
    }
    console.error(`[chzzk-load] Warning: CHZZK_LOAD_BIN is set to "${customPath}" but file does not exist.`);
  }

  const spec = getPlatformSpec();
  if (!spec) {
    return null;
  }

  const { pkgName, binName } = spec;

  // 2. Try require.resolve
  try {
    const resolvedPath = require.resolve(`${pkgName}/bin/${binName}`);
    if (fs.existsSync(resolvedPath)) {
      return resolvedPath;
    }
  } catch (_) {
    // Package not found via require.resolve
  }

  // 3. Search in relative node_modules locations
  const candidates = [
    path.join(__dirname, '..', '..', pkgName, 'bin', binName),
    path.join(__dirname, '..', '..', 'node_modules', pkgName, 'bin', binName),
    path.join(__dirname, '..', 'node_modules', pkgName, 'bin', binName),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

function printTroubleshooting(spec) {
  const current = `${process.platform}-${process.arch}`;
  console.error('\n[chzzk-load] Error: Could not find native binary for your platform (' + current + ').');
  console.error('[chzzk-load] 오류: 현재 플랫폼(' + current + ')에 해당하는 바이너리를 찾을 수 없습니다.\n');

  if (!spec) {
    console.error('Supported platforms / 지원 플랫폼:');
    console.error('  - Windows x64 (win32-x64)');
    console.error('  - Linux x64 (linux-x64)');
    console.error('  - Linux ARM64 (linux-arm64)');
    console.error('  - macOS x64 (darwin-x64)');
    console.error('  - macOS Apple Silicon (darwin-arm64)\n');
  } else {
    console.error('Expected package: ' + spec.pkgName);
    console.error('This typically occurs if optional dependencies were skipped (e.g., using --no-optional).\n');
  }

  console.error('Troubleshooting options / 해결 방법:');
  console.error('  1. Reinstall with optional dependencies enabled:');
  console.error('     npm install -g chzzk-load');
  console.error('  2. Or download the standalone binary directly from GitHub Releases:');
  console.error('     https://github.com/ghfhffh12345/chzzk-load/releases');
  console.error('  3. Or set CHZZK_LOAD_BIN to point to a local chzzk-load binary:');
  console.error('     export CHZZK_LOAD_BIN=/path/to/chzzk-load (Linux/macOS)');
  console.error('     $env:CHZZK_LOAD_BIN="C:\\path\\to\\chzzk-load.exe" (PowerShell)\n');
}

function main() {
  const spec = getPlatformSpec();
  const binPath = findBinary();

  if (!binPath) {
    printTroubleshooting(spec);
    process.exit(1);
  }

  if (process.platform !== 'win32') {
    try {
      fs.chmodSync(binPath, 0o755);
    } catch (_) {
      // Ignore chmod errors if file is not writable or already executable
    }
  }

  const child = spawn(binPath, process.argv.slice(2), {
    stdio: 'inherit',
  });

  process.on('SIGINT', () => child.kill('SIGINT'));
  process.on('SIGTERM', () => child.kill('SIGTERM'));

  child.on('error', (err) => {
    console.error('[chzzk-load] Failed to start native process:', err);
    process.exit(1);
  });

  child.on('exit', (code, signal) => {
    if (code !== null) {
      process.exit(code);
    }
    if (signal) {
      process.kill(process.pid, signal);
    }
  });
}

main();
