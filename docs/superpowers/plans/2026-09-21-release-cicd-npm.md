# CI/CD Release Pipeline & npm CLI Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and verify a multi-platform release CI/CD pipeline using GitHub Actions to publish standalone binaries to GitHub Releases and distribute `chzzk-load` via npm with optional platform-specific binary packages.

**Architecture:** Multi-runner matrix (Windows MSVC, Linux musl with `cargo-zigbuild`, macOS dual-arch with Xcode) generating release archives and SHA-256 checksums; paired with a modern Node.js wrapper CLI package (`chzzk-load`) and 5 platform packages (`chzzk-load-<os>-<arch>`) via `optionalDependencies`.

**Tech Stack:** GitHub Actions, Rust (2024 edition), `cargo-zigbuild`, Node.js, npm, softprops/action-gh-release.

**Spec:** [`docs/superpowers/specs/2026-09-21-release-cicd-npm-design.md`](file:///C:/Users/official/Documents/Code/chzzk-load/docs/superpowers/specs/2026-09-21-release-cicd-npm-design.md)

## Global Constraints

- Standalone executable names: `chzzk-load.exe` (Windows), `chzzk-load` (Linux/macOS).
- Target triples: `x86_64-pc-windows-msvc`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-apple-darwin`, `aarch64-apple-darwin`.
- No Docker/QEMU overhead: Linux builds MUST use `cargo-zigbuild` on `ubuntu-latest`.
- Zero terminal corruption: `bin/chzzk-load.js` MUST forward `SIGINT`, `SIGTERM`, and child exit codes directly.
- Preservation of existing invariants: Keep all existing 69 unit/integration tests passing.

---

### Task 1: npm Wrapper Package & Binary Launcher Script

**Files:**
- Create: `npm/chzzk-load/bin/chzzk-load.js`
- Create: `npm/chzzk-load/package.json`
- Create: `npm/chzzk-load/README.md`

**Interfaces:**
- Consumes: Node.js runtime (`process.platform`, `process.arch`, `child_process.spawn`).
- Produces: `chzzk-load` CLI launcher capable of executing local or package-provided native binary with transparent signal and stdio pass-through.

- [ ] **Step 1: Create npm wrapper directory layout and base package.json**

Create `npm/chzzk-load/package.json`:
```json
{
  "name": "chzzk-load",
  "version": "0.1.0",
  "description": "High-performance TUI for recording Naver Chzzk streams and streaming to Google Drive",
  "bin": {
    "chzzk-load": "./bin/chzzk-load.js"
  },
  "keywords": [
    "chzzk",
    "naver",
    "recorder",
    "livestream",
    "tui",
    "google-drive",
    "cli"
  ],
  "author": "ghfhffh12345",
  "license": "Apache-2.0",
  "repository": {
    "type": "git",
    "url": "https://github.com/ghfhffh12345/chzzk-load.git"
  },
  "bugs": {
    "url": "https://github.com/ghfhffh12345/chzzk-load/issues"
  },
  "homepage": "https://github.com/ghfhffh12345/chzzk-load#readme",
  "engines": {
    "node": ">=16.0.0"
  },
  "optionalDependencies": {
    "chzzk-load-win32-x64": "0.1.0",
    "chzzk-load-linux-x64": "0.1.0",
    "chzzk-load-linux-arm64": "0.1.0",
    "chzzk-load-darwin-x64": "0.1.0",
    "chzzk-load-darwin-arm64": "0.1.0"
  }
}
```

- [ ] **Step 2: Implement robust `bin/chzzk-load.js` launcher**

Create `npm/chzzk-load/bin/chzzk-load.js`:
- Map `process.platform` and `process.arch` to platform package name.
- Search candidates:
  1. Direct package require `require.resolve(pkgName + '/bin/' + binName)`
  2. Relative path in `node_modules` (handling pnpm/yarn hoisted scenarios)
  3. Local development override via `CHZZK_LOAD_BIN` environment variable
- If executable exists:
  - Check file permissions and chmod +x on Unix
  - Spawn with `stdio: 'inherit'`
  - Forward signals (`SIGINT`, `SIGTERM`)
  - Handle exit code forwarding
- If executable not found:
  - Print descriptive troubleshooting message in English/Korean with GitHub Releases link and exit with code 1.

- [ ] **Step 3: Create `npm/chzzk-load/README.md`**

Provide clear usage instructions:
- Running via `npx chzzk-load`
- Installing globally: `npm install -g chzzk-load`
- Prerequisites: FFmpeg on PATH.

- [ ] **Step 4: Verify launcher script syntax with Node.js**

Run: `node -c npm/chzzk-load/bin/chzzk-load.js`
Expected: Exits with code 0 (valid JavaScript syntax).

- [ ] **Step 5: Commit Task 1 changes**

```bash
git add npm/
git commit -m "feat(npm): add wrapper package and binary launcher script"
```

---

### Task 2: npm Packaging Helper & Automated Test Suite

**Files:**
- Create: `scripts/prepare-npm.js`
- Create: `scripts/test-npm-packages.js`

**Interfaces:**
- Consumes: Built native binary (e.g. `target/debug/chzzk-load.exe`), platform definitions.
- Produces: Valid, staged npm packages in `npm/platforms/chzzk-load-<os>-<arch>` and updated `npm/chzzk-load/package.json`.
- Test verification: Comprehensive automated smoke tests validating package structure, package.json schemas, binary discovery, and argument pass-through.

- [ ] **Step 1: Write automated test suite `scripts/test-npm-packages.js`**

Implement tests covering:
1. `prepare-npm.js` execution with `--dry-run` or staging into a temporary directory.
2. Verification of all 5 platform `package.json` schemas (`name`, `version`, `os`, `cpu`, `bin`, `license`).
3. Verification that root `package.json` `optionalDependencies` versions match the package version.
4. Execution of `npm/chzzk-load/bin/chzzk-load.js --version` with a mock or existing binary, verifying stdout and exit code 0.
5. Verification of failure message when binary is missing (exit code 1).

- [ ] **Step 2: Run test suite to verify it fails before `prepare-npm.js` exists**

Run: `node scripts/test-npm-packages.js`
Expected: FAIL with `Cannot find module .../prepare-npm.js` or file not found.

- [ ] **Step 3: Implement `scripts/prepare-npm.js`**

Implement CLI arguments and logic:
- `--version <ver>`: Explicit version override (or reads `Cargo.toml`).
- `--bin-dir <dir>`: Directory containing compiled binaries.
- `--out-dir <dir>`: Output directory for staged packages (defaults to `npm/`).
- `--platforms <list>`: Comma-separated list of platforms to package (or `all`).
- Generates `package.json` for each platform:
  - `win32-x64`: os `["win32"]`, cpu `["x64"]`, bin `{"chzzk-load": "./bin/chzzk-load.exe"}`
  - `linux-x64`: os `["linux"]`, cpu `["x64"]`, bin `{"chzzk-load": "./bin/chzzk-load"}`
  - `linux-arm64`: os `["linux"]`, cpu `["arm64"]`, bin `{"chzzk-load": "./bin/chzzk-load"}`
  - `darwin-x64`: os `["darwin"]`, cpu `["x64"]`, bin `{"chzzk-load": "./bin/chzzk-load"}`
  - `darwin-arm64`: os `["darwin"]`, cpu `["arm64"]`, bin `{"chzzk-load": "./bin/chzzk-load"}`
- Copies binary files into each package's `bin/` directory.
- Updates `npm/chzzk-load/package.json` version and `optionalDependencies` pins.

- [ ] **Step 4: Run automated test suite to verify it passes**

Run: `node scripts/test-npm-packages.js`
Expected: PASS (All test assertions pass cleanly).

- [ ] **Step 5: Commit Task 2 changes**

```bash
git add scripts/
git commit -m "feat(scripts): add npm package preparation and test suite"
```

---

### Task 3: Continuous Integration Workflow (`.github/workflows/ci.yml`)

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Push and pull request events on `main`.
- Produces: Verified build status running formatting, clippy, unit tests on multiple OS runners, and npm packaging test scripts.

- [ ] **Step 1: Define `.github/workflows/ci.yml`**

Create `.github/workflows/ci.yml` with:
- Triggers:
  ```yaml
  name: CI
  on:
    push:
      branches: [ main ]
    pull_request:
      branches: [ main ]
  ```
- Jobs:
  - `lint`:
    - OS: `ubuntu-latest`
    - Steps: Checkout, install stable rust with `rustfmt` and `clippy`, cache cargo, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`.
  - `test`:
    - Strategy matrix: `[ubuntu-latest, windows-latest]`
    - Steps: Checkout, install stable rust, cache cargo, `cargo test --all-targets`.
  - `npm-test`:
    - OS: `ubuntu-latest`
    - Steps: Checkout, setup Node.js `20`, install dependencies if any, run `node scripts/test-npm-packages.js`.

- [ ] **Step 2: Validate YAML syntax locally**

Verify using a parser script:
Run: `node -e "const fs = require('fs'); const yaml = fs.readFileSync('.github/workflows/ci.yml', 'utf8'); console.log('YAML length:', yaml.length);"`
Expected: Success with non-zero length.

- [ ] **Step 3: Commit Task 3 changes**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions CI workflow for linting, tests, and npm verification"
```

---

### Task 4: Multi-Platform Release Workflow (`.github/workflows/release.yml`)

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: Tag pushes matching `v*` and `workflow_dispatch`.
- Produces: GitHub Release with compiled `.zip`/`.tar.gz` archives and `SHA256SUMS.txt`; published npm packages (`chzzk-load` and 5 platform packages).

- [ ] **Step 1: Write `.github/workflows/release.yml`**

Include:
- Triggers:
  ```yaml
  name: Release
  on:
    push:
      tags:
        - 'v*'
    workflow_dispatch:
      inputs:
        tag:
          description: 'Release tag (e.g. v0.1.0)'
          required: false
  ```
- Permissions:
  ```yaml
  permissions:
    contents: write
    id-token: write
  ```
- Job 1: `build-linux` on `ubuntu-latest`:
  - Installs `zig` (`mlugg/setup-zig@v1`) and `cargo-zigbuild` (`pip install cargo-zigbuild`).
  - Adds targets: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`.
  - Builds release binaries with `cargo zigbuild --release --target <target>`.
  - Strips binaries using `strip`.
  - Archives into `chzzk-load-linux-x64.tar.gz` and `chzzk-load-linux-arm64.tar.gz`.
  - Calculates SHA256 sums.
  - Uploads artifacts (archives, checksums, and raw binaries for npm packaging).
- Job 2: `build-macos` on `macos-14`:
  - Adds targets: `x86_64-apple-darwin`, `aarch64-apple-darwin`.
  - Builds release binaries with `cargo build --release --target <target>`.
  - Archives into `chzzk-load-darwin-x64.tar.gz` and `chzzk-load-darwin-arm64.tar.gz`.
  - Calculates SHA256 sums.
  - Uploads artifacts.
- Job 3: `build-windows` on `windows-latest`:
  - Builds `x86_64-pc-windows-msvc` with `cargo build --release`.
  - Archives into `chzzk-load-windows-x64.zip`.
  - Calculates SHA256 sum.
  - Uploads artifacts.
- Job 4: `github-release`:
  - Needs: `[build-linux, build-macos, build-windows]`.
  - Downloads all archives and checksum files.
  - Concatenates into a single consolidated `SHA256SUMS.txt`.
  - Releases via `softprops/action-gh-release@v2` with `generate_release_notes: true`.
- Job 5: `publish-npm`:
  - Needs: `[build-linux, build-macos, build-windows]`.
  - Downloads raw binaries.
  - Sets up Node.js with `registry-url: 'https://registry.npmjs.org/'`.
  - Runs `node scripts/prepare-npm.js --version <tag-version> --bin-dir ./artifacts`.
  - If `NPM_TOKEN` secret is set:
    - Publishes each platform package: `npm publish npm/platforms/<pkg> --access public`.
    - Publishes root package: `npm publish npm/chzzk-load --access public --provenance`.
  - If `NPM_TOKEN` is not set:
    - Executes dry run: `npm pack` on each package to verify packaging without failing.

- [ ] **Step 2: Validate workflow syntax and references**

Verify file syntax and structure.

- [ ] **Step 3: Commit Task 4 changes**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add multi-platform release workflow with cargo-zigbuild and npm publish"
```

---

### Task 5: Documentation Updates & End-to-End Verification

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: Completed packaging and workflow infrastructure.
- Produces: Updated user-facing and developer documentation reflecting the release and npm CLI workflows.

- [ ] **Step 1: Update `README.md`**

Add:
- Installation via `npx chzzk-load` and `npm install -g chzzk-load`.
- Installation via GitHub Releases (pre-built standalone binaries).
- Release badges for GitHub Release and npm version.

- [ ] **Step 2: Update `AGENTS.md`**

Add:
- CI/CD workflow explanation.
- npm release mechanics (`prepare-npm.js`, `optionalDependencies` platform structure).
- Developer commands for testing npm packages locally.

- [ ] **Step 3: Run full local verification test suite**

Run:
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`
- `cargo fmt --check`
- `node scripts/test-npm-packages.js`
Expected: All tests pass with 0 errors and 0 warnings.

- [ ] **Step 4: Commit Task 5 changes**

```bash
git add README.md AGENTS.md
git commit -m "docs: document npm CLI usage and CI/CD release workflow"
```
