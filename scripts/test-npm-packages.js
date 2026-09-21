#!/usr/bin/env node

/**
 * Automated test suite for npm packaging and launcher script.
 * Validates:
 * 1. scripts/prepare-npm.js package staging into temporary directory.
 * 2. Schema compliance of all 5 platform package.json files (os, cpu, bin, license, repo).
 * 3. Root package.json optionalDependencies version synchronization.
 * 4. Launcher execution with CHZZK_LOAD_BIN and staged platform packages (--version, --help).
 * 5. Error handling and bilingual troubleshooting output when native binary is missing.
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawnSync } = require('child_process');
const assert = require('assert');

// Step 1: Ensure prepare-npm.js exists.
// Under TDD, this fails before prepare-npm.js is implemented.
const PREPARE_SCRIPT = require.resolve('./prepare-npm.js');

const REPO_ROOT = path.resolve(__dirname, '..');
const LAUNCHER_SCRIPT = path.join(REPO_ROOT, 'npm', 'chzzk-load', 'bin', 'chzzk-load.js');
const CARGO_TOML = path.join(REPO_ROOT, 'Cargo.toml');

const EXPECTED_PLATFORMS = [
  { name: 'chzzk-load-windows-x64', os: 'win32', cpu: 'x64', bin: 'chzzk-load.exe' },
  { name: 'chzzk-load-linux-x64', os: 'linux', cpu: 'x64', bin: 'chzzk-load' },
  { name: 'chzzk-load-linux-arm64', os: 'linux', cpu: 'arm64', bin: 'chzzk-load' },
  { name: 'chzzk-load-darwin-x64', os: 'darwin', cpu: 'x64', bin: 'chzzk-load' },
  { name: 'chzzk-load-darwin-arm64', os: 'darwin', cpu: 'arm64', bin: 'chzzk-load' },
];

let totalTests = 0;
let passedTests = 0;
let failedTests = 0;

function runTest(name, fn) {
  totalTests++;
  try {
    fn();
    passedTests++;
    console.log(`  [PASS] ${name}`);
  } catch (err) {
    failedTests++;
    console.error(`  [FAIL] ${name}`);
    console.error(`         ${err.message}`);
    if (err.stack) {
      console.error(err.stack.split('\n').slice(1, 4).map(l => '         ' + l.trim()).join('\n'));
    }
  }
}

/**
 * Creates or locates an executable binary for testing on the current platform.
 */
function getOrCreateTestBinary(tempDir) {
  // 1. Check existing target builds
  const candidates = [
    process.env.CHZZK_LOAD_BIN,
    path.join(REPO_ROOT, 'target', 'release', process.platform === 'win32' ? 'chzzk-load.exe' : 'chzzk-load'),
    path.join(REPO_ROOT, 'target', 'debug', process.platform === 'win32' ? 'chzzk-load.exe' : 'chzzk-load'),
  ];

  for (const cand of candidates) {
    if (cand && fs.existsSync(cand)) {
      return cand;
    }
  }

  // 2. Create mock executable script for Unix
  if (process.platform !== 'win32') {
    const mockBinPath = path.join(tempDir, 'mock-chzzk-load');
    const scriptContent = [
      '#!/bin/sh',
      'if [ "$1" = "--version" ] || [ "$1" = "-V" ]; then',
      '  echo "chzzk-load 0.1.0"',
      '  exit 0',
      'fi',
      'if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then',
      '  echo "Real-time Chzzk stream recording and Google Drive syncing"',
      '  echo "Usage: chzzk-load [OPTIONS]"',
      '  exit 0',
      'fi',
      'echo "mock binary running with args: $*"',
      'exit 0',
    ].join('\n');

    fs.writeFileSync(mockBinPath, scriptContent, { mode: 0o755 });
    return mockBinPath;
  }

  // 3. On Windows without target binary, compile a minimal mock using rustc if available
  const mockExePath = path.join(tempDir, 'mock-chzzk-load.exe');
  const rustSource = path.join(tempDir, 'mock.rs');
  const mockCode = `
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("chzzk-load 0.1.0");
        std::process::exit(0);
    }
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("Real-time Chzzk stream recording and Google Drive syncing");
        println!("Usage: chzzk-load.exe [OPTIONS]");
        std::process::exit(0);
    }
    std::process::exit(0);
}
`;
  try {
    fs.writeFileSync(rustSource, mockCode);
    const compileResult = spawnSync('rustc', ['-O', '-o', mockExePath, rustSource]);
    if (compileResult.status === 0 && fs.existsSync(mockExePath)) {
      return mockExePath;
    }
  } catch (_) {
    // rustc not available
  }

  return null;
}

console.log('\n=== Running npm Package & Launcher Automated Tests ===\n');

// Test 1: prepare-npm.js stages packages into a temporary directory
const tmpStageDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-stage-test-'));

runTest('prepare-npm.js execution with --dry-run and --out-dir', () => {
  const result = spawnSync('node', [
    PREPARE_SCRIPT,
    '--out-dir', tmpStageDir,
    '--version', '0.9.9',
    '--dry-run',
  ], { encoding: 'utf8' });

  assert.strictEqual(
    result.status,
    0,
    `prepare-npm.js failed with code ${result.status}:\n${result.stderr || result.stdout}`
  );
});

// Test 2: Verify all 5 platform package.json schemas
EXPECTED_PLATFORMS.forEach((expected) => {
  runTest(`Platform package schema: ${expected.name}`, () => {
    const pkgDir = path.join(tmpStageDir, 'platforms', expected.name);
    const pkgJsonPath = path.join(pkgDir, 'package.json');

    assert.ok(fs.existsSync(pkgJsonPath), `Missing package.json for ${expected.name}`);

    const pkg = JSON.parse(fs.readFileSync(pkgJsonPath, 'utf8'));

    assert.strictEqual(pkg.name, expected.name, 'name mismatch');
    assert.strictEqual(pkg.version, '0.9.9', 'version mismatch');
    assert.deepStrictEqual(pkg.os, [expected.os], 'os array mismatch');
    assert.deepStrictEqual(pkg.cpu, [expected.cpu], 'cpu array mismatch');
    assert.strictEqual(pkg.license, 'Apache-2.0', 'license mismatch');
    assert.ok(pkg.repository && pkg.repository.url, 'repository url missing');

    const expectedBinPath = `./bin/${expected.bin}`;
    assert.strictEqual(
      pkg.bin && pkg.bin['chzzk-load'],
      expectedBinPath,
      `bin mismatch: expected ${expectedBinPath}, got ${pkg.bin ? pkg.bin['chzzk-load'] : undefined}`
    );

    // Verify binary placeholder or file exists
    const binFilePath = path.join(pkgDir, 'bin', expected.bin);
    assert.ok(fs.existsSync(binFilePath), `Binary file not created: ${binFilePath}`);
  });
});

// Test 3: Verify root package.json optionalDependencies version pins
runTest('Root package.json optionalDependencies and version update', () => {
  const rootPkgPath = path.join(tmpStageDir, 'chzzk-load', 'package.json');
  assert.ok(fs.existsSync(rootPkgPath), `Root package.json not found in output directory: ${rootPkgPath}`);

  const rootPkg = JSON.parse(fs.readFileSync(rootPkgPath, 'utf8'));
  assert.strictEqual(rootPkg.version, '0.9.9', 'Root package version was not updated to 0.9.9');
  assert.ok(rootPkg.optionalDependencies, 'optionalDependencies missing from root package.json');

  EXPECTED_PLATFORMS.forEach((p) => {
    assert.strictEqual(
      rootPkg.optionalDependencies[p.name],
      '0.9.9',
      `optionalDependencies pin for ${p.name} mismatch`
    );
  });
});

// Test 4: prepare-npm.js reads version from Cargo.toml when --version is omitted
runTest('prepare-npm.js version fallback to Cargo.toml', () => {
  const tmpCargoStageDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-cargo-test-'));

  const cargoContent = fs.readFileSync(CARGO_TOML, 'utf8');
  const match = cargoContent.match(/\[package\][^]*?version\s*=\s*"([^"]+)"/);
  assert.ok(match, 'Could not read version from Cargo.toml');
  const expectedCargoVersion = match[1];

  const result = spawnSync('node', [
    PREPARE_SCRIPT,
    '--out-dir', tmpCargoStageDir,
    '--dry-run',
  ], { encoding: 'utf8' });

  assert.strictEqual(result.status, 0, `prepare-npm failed: ${result.stderr}`);

  const stagedRootPkg = JSON.parse(
    fs.readFileSync(path.join(tmpCargoStageDir, 'chzzk-load', 'package.json'), 'utf8')
  );
  assert.strictEqual(stagedRootPkg.version, expectedCargoVersion, 'Did not match Cargo.toml version');

  const stagedWinPkg = JSON.parse(
    fs.readFileSync(path.join(tmpCargoStageDir, 'platforms', 'chzzk-load-windows-x64', 'package.json'), 'utf8')
  );
  assert.strictEqual(stagedWinPkg.version, expectedCargoVersion, 'Platform version did not match Cargo.toml');
});

// Test 5: prepare-npm.js with --bin-dir copying real or mock binaries
runTest('prepare-npm.js copies binaries from --bin-dir', () => {
  const tmpBinDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-mockbin-'));
  const tmpOutDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-copy-test-'));

  // Create mock binaries in bin-dir
  EXPECTED_PLATFORMS.forEach((p) => {
    // artifact names: chzzk-load.exe or chzzk-load-<os>-<arch>
    const artifactName = p.name === 'chzzk-load-windows-x64' ? 'chzzk-load.exe' : p.name;
    const binPath = path.join(tmpBinDir, artifactName);
    fs.writeFileSync(binPath, `BINARY_DATA_FOR_${p.name}`);
  });

  const result = spawnSync('node', [
    PREPARE_SCRIPT,
    '--bin-dir', tmpBinDir,
    '--out-dir', tmpOutDir,
    '--version', '1.0.0',
  ], { encoding: 'utf8' });

  assert.strictEqual(result.status, 0, `prepare-npm.js with bin-dir failed: ${result.stderr}`);

  EXPECTED_PLATFORMS.forEach((p) => {
    const targetFile = path.join(tmpOutDir, 'platforms', p.name, 'bin', p.bin);
    assert.ok(fs.existsSync(targetFile), `Target binary missing: ${targetFile}`);
    const content = fs.readFileSync(targetFile, 'utf8');
    assert.strictEqual(content, `BINARY_DATA_FOR_${p.name}`, `Binary content mismatch for ${p.name}`);
  });
});

// Test 6: Execution test of launcher with CHZZK_LOAD_BIN
const testBinary = getOrCreateTestBinary(tmpStageDir);

if (testBinary) {
  runTest('Launcher execution: CHZZK_LOAD_BIN with --version', () => {
    const result = spawnSync('node', [LAUNCHER_SCRIPT, '--version'], {
      env: { ...process.env, CHZZK_LOAD_BIN: testBinary },
      encoding: 'utf8',
    });

    assert.strictEqual(result.status, 0, `Expected exit code 0, got ${result.status}. Error: ${result.stderr}`);
    const output = (result.stdout + result.stderr).toLowerCase();
    assert.ok(output.includes('chzzk-load'), `Expected output to contain 'chzzk-load', got:\n${result.stdout}`);
  });

  runTest('Launcher execution: CHZZK_LOAD_BIN with --help', () => {
    const result = spawnSync('node', [LAUNCHER_SCRIPT, '--help'], {
      env: { ...process.env, CHZZK_LOAD_BIN: testBinary },
      encoding: 'utf8',
    });

    assert.strictEqual(result.status, 0, `Expected exit code 0, got ${result.status}. Error: ${result.stderr}`);
    const output = (result.stdout + result.stderr).toLowerCase();
    assert.ok(
      output.includes('usage') || output.includes('options') || output.includes('help'),
      `Expected output to contain usage/options, got:\n${result.stdout}`
    );
  });
} else {
  console.log('  [SKIP] Skipping CHZZK_LOAD_BIN execution tests (no native binary available on current platform)');
}

// Test 7: Execution test of launcher in staged node_modules environment
if (testBinary) {
  runTest('Launcher execution: discovered via staged node_modules layout', () => {
    const tmpEnvDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-env-test-'));
    const nodeModules = path.join(tmpEnvDir, 'node_modules');
    const mainPkgDir = path.join(nodeModules, 'chzzk-load');
    const currentPkgName = `chzzk-load-${process.platform}-${process.arch}`;
    const currentBinName = process.platform === 'win32' ? 'chzzk-load.exe' : 'chzzk-load';
    const platformPkgDir = path.join(nodeModules, currentPkgName);

    // Copy launcher package to node_modules/chzzk-load
    fs.mkdirSync(path.join(mainPkgDir, 'bin'), { recursive: true });
    fs.copyFileSync(LAUNCHER_SCRIPT, path.join(mainPkgDir, 'bin', 'chzzk-load.js'));

    // Create staged platform package in node_modules/<pkgName>/bin/<binName>
    fs.mkdirSync(path.join(platformPkgDir, 'bin'), { recursive: true });
    const targetBinary = path.join(platformPkgDir, 'bin', currentBinName);
    fs.copyFileSync(testBinary, targetBinary);
    if (process.platform !== 'win32') {
      fs.chmodSync(targetBinary, 0o755);
    }

    const stagedLauncher = path.join(mainPkgDir, 'bin', 'chzzk-load.js');
    const envClean = { ...process.env };
    delete envClean.CHZZK_LOAD_BIN;

    const result = spawnSync('node', [stagedLauncher, '--version'], {
      cwd: tmpEnvDir,
      env: envClean,
      encoding: 'utf8',
    });

    assert.strictEqual(result.status, 0, `Failed to execute via staged node_modules: ${result.stderr || result.stdout}`);
    const output = (result.stdout + result.stderr).toLowerCase();
    assert.ok(output.includes('chzzk-load'), `Output should contain chzzk-load: ${result.stdout}`);
  });
}

// Test 8: prepare-npm.js filters platforms with --platforms
runTest('prepare-npm.js filters platforms with --platforms', () => {
  const tmpFilterDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chzzk-load-filter-test-'));
  const result = spawnSync('node', [
    PREPARE_SCRIPT,
    '--out-dir', tmpFilterDir,
    '--platforms', 'windows-x64,linux-x64',
    '--dry-run',
  ], { encoding: 'utf8' });

  assert.strictEqual(result.status, 0, `prepare-npm failed: ${result.stderr}`);
  assert.ok(fs.existsSync(path.join(tmpFilterDir, 'platforms', 'chzzk-load-windows-x64')), 'windows-x64 should exist');
  assert.ok(fs.existsSync(path.join(tmpFilterDir, 'platforms', 'chzzk-load-linux-x64')), 'linux-x64 should exist');
  assert.ok(!fs.existsSync(path.join(tmpFilterDir, 'platforms', 'chzzk-load-darwin-x64')), 'darwin-x64 should not exist');
});

// Test 9: Error handling when binary is missing
runTest('Launcher error handling: missing binary prints troubleshooting and exits code 1', () => {
  const envClean = { ...process.env };
  delete envClean.CHZZK_LOAD_BIN;

  const result = spawnSync('node', [LAUNCHER_SCRIPT], {
    env: envClean,
    encoding: 'utf8',
  });

  assert.strictEqual(result.status, 1, `Expected exit code 1 for missing binary, got ${result.status}`);

  const stderr = result.stderr || '';
  // Check English troubleshooting message
  assert.ok(
    stderr.includes('Could not find native binary for your platform'),
    'Expected English error message in stderr'
  );
  // Check Korean troubleshooting message
  assert.ok(
    stderr.includes('오류: 현재 플랫폼'),
    'Expected Korean error message in stderr'
  );
  // Check GitHub releases link
  assert.ok(
    stderr.includes('https://github.com/ghfhffh12345/chzzk-load/releases'),
    'Expected GitHub Releases URL in troubleshooting'
  );
  // Check CHZZK_LOAD_BIN hint
  assert.ok(
    stderr.includes('CHZZK_LOAD_BIN'),
    'Expected CHZZK_LOAD_BIN mention in troubleshooting'
  );
});

// Summary and cleanup
console.log('\n----------------------------------------------------');
console.log(`Results: ${passedTests} passed, ${failedTests} failed, ${totalTests} total.`);
console.log('----------------------------------------------------\n');

// Clean up temporary stage directory
try {
  fs.rmSync(tmpStageDir, { recursive: true, force: true });
} catch (_) {}

if (failedTests > 0) {
  process.exit(1);
} else {
  console.log('All npm packaging and launcher tests passed successfully!\n');
  process.exit(0);
}
