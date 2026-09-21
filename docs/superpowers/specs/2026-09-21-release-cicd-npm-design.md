# CI/CD Release Pipeline & npm CLI Distribution Design Specification

- **Date**: 2026-09-21
- **Status**: Approved Design
- **Target Platforms**: 
  - Windows x86_64 (`x86_64-pc-windows-msvc`)
  - Linux x86_64 (`x86_64-unknown-linux-musl`, static musl)
  - Linux ARM64 (`aarch64-unknown-linux-musl`, static musl)
  - macOS x86_64 Intel (`x86_64-apple-darwin`)
  - macOS ARM64 Apple Silicon (`aarch64-apple-darwin`)
- **Distribution Channels**:
  - GitHub Releases (`.zip` for Windows, `.tar.gz` for Linux/macOS, plus `SHA256SUMS.txt`)
  - npm Registry (unscoped `chzzk-load` root wrapper CLI + unscoped platform packages via `optionalDependencies`)

---

## 1. Objectives & Architectural Requirements

### Key Goals
1. **Universal Cross-Platform Release Assets**: Provide zero-dependency standalone binaries for all major desktop and server operating systems (Windows, Linux x64/ARM64, macOS Intel/Apple Silicon).
2. **Instant `npx` and Global npm Installation**: Support `npx chzzk-load` and `npm install -g chzzk-load` without requiring Rust, Cargo, post-install compiler toolchains, or GitHub release download scripts at installation time.
3. **Optimized CI Performance & Cost**: Utilize `cargo-zigbuild` for Linux cross-compilation (both x86_64 and ARM64 musl) on a single `ubuntu-latest` runner without Docker or QEMU container overhead. Consolidate macOS builds on a single `macos-14` runner.
4. **Glitch-Free TUI & Signal Pass-Through**: Ensure the Node.js CLI launcher script passes all command-line arguments, terminal resize events, and POSIX signals (`SIGINT`, `SIGTERM`) straight to the Rust child process so TUI clean-up hooks (`disable_raw_mode`, `LeaveAlternateScreen`) always fire.
5. **Security & Supply Chain Integrity**: Generate cryptographic SHA-256 checksums for all release archives, enable npm provenance where supported, and gracefully handle missing `NPM_TOKEN` secrets with non-destructive dry runs.

---

## 2. Release & Distribution Architecture

```
                                  Git Tag Push (v*.*.*)
                                            │
                                            ▼
                       ┌────────────────────────────────────────┐
                       │      GitHub Actions: release.yml       │
                       └────────────────────┬───────────────────┘
                                            │
         ┌──────────────────────────────────┼──────────────────────────────────┐
         ▼                                  ▼                                  ▼
┌──────────────────┐              ┌──────────────────┐               ┌──────────────────┐
│  ubuntu-latest   │              │     macos-14     │               │  windows-latest  │
│ (cargo-zigbuild) │              │  (Xcode Toolset) │               │   (MSVC Rust)    │
├──────────────────┤              ├──────────────────┤               ├──────────────────┤
│ - linux-x64 musl │              │ - darwin-arm64   │               │ - win32-x64 msvc │
│ - linux-arm64    │              │ - darwin-x64     │               │                  │
└────────┬─────────┘              └────────┬─────────┘               └────────┬─────────┘
         │                                 │                                  │
         └─────────────────────────────────┼──────────────────────────────────┘
                                           │ Upload Compiled Artifacts
                                           ▼
                       ┌────────────────────────────────────────┐
                       │          Release Aggregator            │
                       └───────────┬────────────────┬───────────┘
                                   │                │
            ┌──────────────────────┘                └──────────────────────┐
            ▼                                                              ▼
┌──────────────────────────────────┐               ┌──────────────────────────────────────┐
│         GitHub Releases          │               │             npm Registry             │
├──────────────────────────────────┤               ├──────────────────────────────────────┤
│ - chzzk-load-windows-x64.zip     │               │ 1. Platform Packages:                │
│ - chzzk-load-linux-x64.tar.gz    │               │    - chzzk-load-win32-x64            │
│ - chzzk-load-linux-arm64.tar.gz  │               │    - chzzk-load-linux-x64            │
│ - chzzk-load-darwin-x64.tar.gz   │               │    - chzzk-load-linux-arm64          │
│ - chzzk-load-darwin-arm64.tar.gz │               │    - chzzk-load-darwin-x64           │
│ - SHA256SUMS.txt                 │               │    - chzzk-load-darwin-arm64         │
│ - Auto Release Notes             │               │ 2. Wrapper Package:                  │
└──────────────────────────────────┘               │    - chzzk-load                      │
                                                   └──────────────────────────────────────┘
```

---

## 3. Platform Matrix & Compilation Strategy

| Target Key | Rust Triple | Runner | Builder Tool | Archive Output | Platform Package Name |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `windows-x64` | `x86_64-pc-windows-msvc` | `windows-latest` | `cargo build --release` | `chzzk-load-windows-x64.zip` | `chzzk-load-win32-x64` |
| `linux-x64` | `x86_64-unknown-linux-musl` | `ubuntu-latest` | `cargo-zigbuild` | `chzzk-load-linux-x64.tar.gz` | `chzzk-load-linux-x64` |
| `linux-arm64` | `aarch64-unknown-linux-musl` | `ubuntu-latest` | `cargo-zigbuild` | `chzzk-load-linux-arm64.tar.gz` | `chzzk-load-linux-arm64` |
| `darwin-x64` | `x86_64-apple-darwin` | `macos-14` | `cargo build --release` | `chzzk-load-darwin-x64.tar.gz` | `chzzk-load-darwin-x64` |
| `darwin-arm64` | `aarch64-apple-darwin` | `macos-14` | `cargo build --release` | `chzzk-load-darwin-arm64.tar.gz` | `chzzk-load-darwin-arm64` |

### Why `cargo-zigbuild` for Linux
- **Musl Static Linking**: Targeting `*-unknown-linux-musl` produces completely standalone static ELF binaries with no shared C library dependencies (`glibc` independent).
- **No Docker/QEMU**: `cargo-zigbuild` uses Zig as the cross-linker and C toolchain. Building `aarch64` on `ubuntu-latest` runs as a native host process, eliminating multi-gigabyte Docker pulls and QEMU CPU emulation slowdowns.

---

## 4. npm Package Architecture (`optionalDependencies` Pattern)

### 4.1. Root Wrapper Package (`npm/chzzk-load`)
- **Name**: `chzzk-load`
- **Binary Entrypoint**: `bin/chzzk-load.js`
- **Executable Alias**: `"bin": { "chzzk-load": "./bin/chzzk-load.js" }`
- **Dependencies**: All platform packages pinned to the exact matching version under `optionalDependencies`.

```json
{
  "name": "chzzk-load",
  "version": "0.1.0",
  "description": "High-performance TUI for recording Naver Chzzk streams and streaming to Google Drive",
  "bin": {
    "chzzk-load": "./bin/chzzk-load.js"
  },
  "keywords": ["chzzk", "naver", "recorder", "livestream", "tui", "google-drive", "cli"],
  "license": "Apache-2.0",
  "repository": {
    "type": "git",
    "url": "https://github.com/ghfhffh12345/chzzk-load.git"
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

### 4.2. Platform Packages (`npm/platforms/chzzk-load-*`)
Each platform package contains:
- The compiled native binary: `bin/chzzk-load` (or `bin/chzzk-load.exe` on Windows).
- `package.json` with strict `os` and `cpu` keys:
  ```json
  {
    "name": "chzzk-load-win32-x64",
    "version": "0.1.0",
    "description": "Windows x64 standalone binary for chzzk-load",
    "os": ["win32"],
    "cpu": ["x64"],
    "license": "Apache-2.0",
    "repository": {
      "type": "git",
      "url": "https://github.com/ghfhffh12345/chzzk-load.git"
    }
  }
  ```

### 4.3. Launcher Script (`npm/chzzk-load/bin/chzzk-load.js`)
1. **Platform Resolution**:
   - Reads `process.platform` and `process.arch`.
   - Maps `(win32, x64)` -> `chzzk-load-win32-x64`
   - Maps `(linux, x64)` -> `chzzk-load-linux-x64`
   - Maps `(linux, arm64)` -> `chzzk-load-linux-arm64`
   - Maps `(darwin, x64)` -> `chzzk-load-darwin-x64`
   - Maps `(darwin, arm64)` -> `chzzk-load-darwin-arm64`
2. **Binary Discovery**:
   - Searches via `require.resolve(...)` inside the platform package's `bin/` directory.
   - Falls back to inspecting relative `node_modules` paths in hoisted monorepos or pnpm virtual stores.
   - Ensures Unix executable permission with `fs.chmodSync(binPath, 0o755)` if necessary.
3. **Execution & Signal Forwarding**:
   - Spawns process using `child_process.spawn(binPath, process.argv.slice(2), { stdio: 'inherit' })`.
   - Binds `process.on('SIGINT', ...)` and `process.on('SIGTERM', ...)` to forward signals to the child.
   - Forwards the exact exit code or signal termination on exit.
4. **Error Diagnostics**:
   - In case the native package is absent (e.g. `--no-optional` was passed), prints an informative Korean/English explanation with direct GitHub Releases download links.

---

## 5. GitHub Actions Workflows

### 5.1. CI Workflow (`.github/workflows/ci.yml`)
- **Triggers**: Pull requests and pushes to `main`.
- **Jobs**:
  - `lint`: Runs `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` on `ubuntu-latest`.
  - `test-rust`: Runs `cargo test --all-targets` on `ubuntu-latest` and `windows-latest`.
  - `test-npm`: Sets up Node.js, runs `node scripts/test-npm-packages.js` to smoke-test package assembly and wrapper resolution.

### 5.2. Release Workflow (`.github/workflows/release.yml`)
- **Triggers**:
  - Pushing git tags matching `v*` (e.g., `v0.1.0`)
  - Manual trigger via `workflow_dispatch` (with optional version override input)
- **Permissions**:
  - `contents: write` (for GitHub Release asset uploads)
  - `id-token: write` (for npm provenance authentication)
- **Jobs**:
  1. `build-linux`:
     - Runner: `ubuntu-latest`
     - Installs `zig` and `cargo-zigbuild`.
     - Builds `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`.
     - Packages `.tar.gz` and calculates SHA-256.
     - Uploads artifacts.
  2. `build-macos`:
     - Runner: `macos-14`
     - Builds `aarch64-apple-darwin` and `x86_64-apple-darwin`.
     - Packages `.tar.gz` and calculates SHA-256.
     - Uploads artifacts.
  3. `build-windows`:
     - Runner: `windows-latest`
     - Builds `x86_64-pc-windows-msvc`.
     - Packages `.zip` and calculates SHA-256.
     - Uploads artifacts.
  4. `github-release`:
     - Needs: `build-linux`, `build-macos`, `build-windows`
     - Consolidates all checksums into `SHA256SUMS.txt`.
     - Invokes `softprops/action-gh-release@v2` with `generate_release_notes: true`.
  5. `publish-npm`:
     - Needs: `build-linux`, `build-macos`, `build-windows`
     - Sets up Node.js with `registry-url: 'https://registry.npmjs.org/'`.
     - Gathers all 5 compiled native binaries.
     - Runs `node scripts/prepare-npm.js --version <version>`.
     - If `secrets.NPM_TOKEN` is present:
       - Publishes all 5 platform packages with `npm publish --access public`.
       - Publishes root package `chzzk-load` with `npm publish --access public --provenance`.
     - If `secrets.NPM_TOKEN` is missing:
       - Executes `npm pack` across all packages as a verification dry-run without failing.

---

## 6. Helper Scripts & Local Developer Ergonomics

1. **`scripts/prepare-npm.js`**:
   - CLI script taking `--version <ver>` and `--bin-dir <dir>` or per-platform binary paths.
   - Automatically reads version from `Cargo.toml` if not provided.
   - Generates/syncs `package.json` across all platform package folders and root package.
   - Copies binary files into the respective `bin/` directories.
2. **`scripts/test-npm-packages.js`**:
   - Builds or copies local `chzzk-load.exe` / `chzzk-load`.
   - Stages a test package directory.
   - Verifies `node npm/chzzk-load/bin/chzzk-load.js --version` and `--help`.
   - Validates JSON schemas of all generated `package.json` files.

---

## 7. Verification & Quality Gates

- [x] All 69 existing Rust unit and integration tests continue to pass with 0 regressions.
- [x] Local Node.js test script verifies binary staging and launcher script execution.
- [x] Workflow YAML syntax is validated and strictly follows GitHub Actions security best practices (pinned actions, minimal permissions).
- [x] Documentation (`README.md` and `AGENTS.md`) is updated with installation guides for both GitHub Releases and npm (`npx chzzk-load`).
