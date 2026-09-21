#!/usr/bin/env node

/**
 * npm Package Preparation Helper for chzzk-load
 *
 * Prepares and stages platform-specific packages under <out-dir>/platforms/
 * and synchronizes version and optionalDependencies in the root package.json.
 *
 * Supported CLI Arguments:
 *   --version <ver>      Explicit version override (defaults to Cargo.toml version)
 *   --bin-dir <dir>      Directory containing compiled native binaries
 *   --out-dir <dir>      Root directory for staged npm packages (defaults to npm/)
 *   --platforms <list>   Comma-separated list of platform names (defaults to all)
 *   --dry-run            Generate package skeletons and placeholders without requiring real binaries
 *   --root-pkg <path>    Explicit path to root package.json to update
 */

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '..');
const DEFAULT_NPM_DIR = path.join(REPO_ROOT, 'npm');
const CARGO_TOML_PATH = path.join(REPO_ROOT, 'Cargo.toml');

const PLATFORMS = [
  { name: 'chzzk-load-win32-x64', os: 'win32', cpu: 'x64', bin: 'chzzk-load.exe', artifactName: 'chzzk-load.exe' },
  { name: 'chzzk-load-linux-x64', os: 'linux', cpu: 'x64', bin: 'chzzk-load', artifactName: 'chzzk-load-linux-x64' },
  { name: 'chzzk-load-linux-arm64', os: 'linux', cpu: 'arm64', bin: 'chzzk-load', artifactName: 'chzzk-load-linux-arm64' },
  { name: 'chzzk-load-darwin-x64', os: 'darwin', cpu: 'x64', bin: 'chzzk-load', artifactName: 'chzzk-load-darwin-x64' },
  { name: 'chzzk-load-darwin-arm64', os: 'darwin', cpu: 'arm64', bin: 'chzzk-load', artifactName: 'chzzk-load-darwin-arm64' },
];

function parseArgs(argv) {
  const args = {
    version: null,
    binDir: null,
    outDir: DEFAULT_NPM_DIR,
    platforms: null,
    dryRun: false,
    rootPkg: null,
  };

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--version' || arg === '-v') {
      args.version = argv[++i];
    } else if (arg === '--bin-dir') {
      args.binDir = argv[++i];
    } else if (arg === '--out-dir') {
      args.outDir = path.resolve(argv[++i]);
    } else if (arg === '--platforms') {
      args.platforms = argv[++i].split(',').map((s) => s.trim());
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--root-pkg') {
      args.rootPkg = path.resolve(argv[++i]);
    } else if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    }
  }

  return args;
}

function printUsage() {
  console.log(`
Usage: node scripts/prepare-npm.js [OPTIONS]

Options:
  --version <ver>      Package version (defaults to version in Cargo.toml)
  --bin-dir <dir>      Directory containing compiled binaries
  --out-dir <dir>      Staging output directory (defaults to npm/)
  --platforms <list>   Comma-separated platforms to build (defaults to all)
  --dry-run            Stage skeletons with placeholder binaries
  --root-pkg <path>    Explicit path to root wrapper package.json
  --help, -h           Show this help message
`);
}

function getVersion(explicitVersion) {
  if (explicitVersion) {
    // Strip leading 'v' if present (e.g. v0.1.0 -> 0.1.0)
    return explicitVersion.replace(/^v/, '');
  }

  if (fs.existsSync(CARGO_TOML_PATH)) {
    const content = fs.readFileSync(CARGO_TOML_PATH, 'utf8');
    const match = content.match(/\[package\][^]*?version\s*=\s*"([^"]+)"/);
    if (match) {
      return match[1];
    }
  }

  throw new Error('Unable to determine version. Specify --version <ver> or ensure Cargo.toml exists.');
}

function findBinaryInBinDir(binDir, platform) {
  if (!binDir || !fs.existsSync(binDir)) {
    return null;
  }

  const candidates = [
    path.join(binDir, platform.artifactName),
    path.join(binDir, platform.bin),
    path.join(binDir, platform.name, platform.bin),
    path.join(binDir, platform.name.replace('chzzk-load-', ''), platform.bin),
    path.join(binDir, platform.name),
  ];

  if (platform.os === 'win32') {
    candidates.push(
      path.join(binDir, 'chzzk-load-windows-x64.exe'),
      path.join(binDir, 'chzzk-load-win32-x64.exe')
    );
  }

  for (const cand of candidates) {
    if (fs.existsSync(cand) && fs.statSync(cand).isFile()) {
      return cand;
    }
  }

  return null;
}

function copyDirectoryRecursive(src, dest) {
  fs.mkdirSync(dest, { recursive: true });
  const entries = fs.readdirSync(src, { withFileTypes: true });

  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);

    if (entry.isDirectory()) {
      copyDirectoryRecursive(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function prepareNpm(options = {}) {
  const version = getVersion(options.version);
  const outDir = options.outDir ? path.resolve(options.outDir) : DEFAULT_NPM_DIR;
  const binDir = options.binDir ? path.resolve(options.binDir) : null;
  const dryRun = Boolean(options.dryRun || !binDir);

  const selectedPlatforms = options.platforms
    ? PLATFORMS.filter(
        (p) =>
          options.platforms.includes(p.name) ||
          options.platforms.includes(p.name.replace('chzzk-load-', ''))
      )
    : PLATFORMS;

  if (selectedPlatforms.length === 0) {
    throw new Error('No matching platforms selected for preparation.');
  }

  console.log(`[prepare-npm] Staging npm packages for version: ${version}`);
  console.log(`[prepare-npm] Output directory: ${outDir}`);
  if (binDir) console.log(`[prepare-npm] Binary source directory: ${binDir}`);
  if (dryRun) console.log('[prepare-npm] Running in dry-run mode (using placeholders if binaries missing)');

  // 1. Stage platform packages
  const platformsDir = path.join(outDir, 'platforms');
  fs.mkdirSync(platformsDir, { recursive: true });

  for (const platform of selectedPlatforms) {
    const pkgDir = path.join(platformsDir, platform.name);
    const targetBinDir = path.join(pkgDir, 'bin');
    fs.mkdirSync(targetBinDir, { recursive: true });

    const targetBinPath = path.join(targetBinDir, platform.bin);

    // Locate binary
    const sourceBin = findBinaryInBinDir(binDir, platform);

    if (sourceBin) {
      console.log(`[prepare-npm]   Copying ${sourceBin} -> ${targetBinPath}`);
      fs.copyFileSync(sourceBin, targetBinPath);
      if (platform.os !== 'win32') {
        try {
          fs.chmodSync(targetBinPath, 0o755);
        } catch (_) {}
      }
    } else if (dryRun) {
      console.log(`[prepare-npm]   Creating placeholder for ${platform.name}`);
      const placeholderContent =
        platform.os === 'win32'
          ? 'MZ-MOCK-BINARY-PLACEHOLDER'
          : `#!/bin/sh\necho "Placeholder binary for ${platform.name}"\nexit 1\n`;
      fs.writeFileSync(targetBinPath, placeholderContent);
      if (platform.os !== 'win32') {
        try {
          fs.chmodSync(targetBinPath, 0o755);
        } catch (_) {}
      }
    } else {
      throw new Error(
        `Binary for platform ${platform.name} (${platform.artifactName}) not found in ${binDir}`
      );
    }

    // Generate platform package.json
    const platformPkg = {
      name: platform.name,
      version: version,
      description: `Native binary distribution of chzzk-load for ${platform.os} (${platform.cpu})`,
      os: [platform.os],
      cpu: [platform.cpu],
      bin: {
        'chzzk-load': `./bin/${platform.bin}`,
      },
      license: 'Apache-2.0',
      repository: {
        type: 'git',
        url: 'git+https://github.com/ghfhffh12345/chzzk-load.git',
      },
      bugs: {
        url: 'https://github.com/ghfhffh12345/chzzk-load/issues',
      },
      homepage: 'https://github.com/ghfhffh12345/chzzk-load#readme',
    };

    const pkgJsonPath = path.join(pkgDir, 'package.json');
    fs.writeFileSync(pkgJsonPath, JSON.stringify(platformPkg, null, 2) + '\n');

    // Generate platform README.md
    const readmePath = path.join(pkgDir, 'README.md');
    const readmeContent = `# ${platform.name}\n\nNative pre-compiled binary distribution of \`chzzk-load\` for ${platform.os} (${platform.cpu}).\n\nThis package is an optional dependency of the main [\`chzzk-load\`](https://www.npmjs.com/package/chzzk-load) package and is not intended to be installed directly.\n\n- Repository: https://github.com/ghfhffh12345/chzzk-load\n- Issues: https://github.com/ghfhffh12345/chzzk-load/issues\n`;
    fs.writeFileSync(readmePath, readmeContent);
  }

  // 2. Update root wrapper package.json
  // Determine root package.json location
  let rootPkgFile;
  const repoRootPkg = path.join(DEFAULT_NPM_DIR, 'chzzk-load', 'package.json');
  const stagedRootPkg = path.join(outDir, 'chzzk-load', 'package.json');

  if (options.rootPkg) {
    rootPkgFile = options.rootPkg;
  } else if (outDir !== DEFAULT_NPM_DIR) {
    // If output is directed to a staging/tmp directory:
    // Ensure outDir/chzzk-load exists by copying template if necessary
    const stagedChzzkDir = path.join(outDir, 'chzzk-load');
    if (!fs.existsSync(stagedRootPkg) && fs.existsSync(path.join(DEFAULT_NPM_DIR, 'chzzk-load'))) {
      copyDirectoryRecursive(path.join(DEFAULT_NPM_DIR, 'chzzk-load'), stagedChzzkDir);
    }
    rootPkgFile = stagedRootPkg;
  } else {
    rootPkgFile = repoRootPkg;
  }

  if (fs.existsSync(rootPkgFile)) {
    console.log(`[prepare-npm] Updating root package: ${rootPkgFile}`);
    const rootPkg = JSON.parse(fs.readFileSync(rootPkgFile, 'utf8'));

    rootPkg.version = version;
    rootPkg.optionalDependencies = rootPkg.optionalDependencies || {};

    for (const p of PLATFORMS) {
      rootPkg.optionalDependencies[p.name] = version;
    }

    fs.writeFileSync(rootPkgFile, JSON.stringify(rootPkg, null, 2) + '\n');
    console.log(`[prepare-npm] Successfully updated root package to version ${version}`);
  } else {
    console.warn(`[prepare-npm] Warning: root package.json not found at ${rootPkgFile}`);
  }

  console.log('[prepare-npm] Packaging staging complete.\n');
}

function main() {
  const args = parseArgs(process.argv);
  try {
    prepareNpm(args);
  } catch (err) {
    console.error(`[prepare-npm] Error: ${err.message}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  PLATFORMS,
  prepareNpm,
  getVersion,
  parseArgs,
};
