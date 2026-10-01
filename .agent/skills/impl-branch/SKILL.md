---
name: impl-branch
description: Creates a dedicated feature branch from an implementation plan and executes the plan with incremental, meaningful git commits as each milestone or task is completed, preventing massive end-of-turn commit batches.
---

# Implementation Branch Workflow (`impl-branch`)

Use this skill when instructed to implement a design specification, architectural plan, or roadmap (e.g., `impl-branch plans/YYYY-MM-DD-<topic>.md`).

This workflow ensures work occurs on an isolated branch and commits changes **incrementally as work progresses**, producing a clean, bisectable commit history.

---

## Workflow Steps

### 1. Ingest Plan & Establish Feature Branch

1. **Read the Target Plan**:
   - Locate and parse the requested plan file (`plans/YYYY-MM-DD-<topic>.md`).
   - Extract the high-level goal, target branch name or slug, and task checklists.

2. **Verify Working Tree Safety**:
   ```bash
   git status -u
   ```
   Ensure the current working tree is clean. If uncommitted changes exist, pause and ask the user to commit or stash them first.

3. **Create and Switch to Feature Branch**:
   - Derive a clean branch name:
     - Plans labeled features/readiness: `feat/<slug>` (e.g., `feat/v1.0.0-readiness`)
     - Plans labeled refactors/fixes: `refactor/<slug>` or `fix/<slug>`
   ```bash
   git checkout -b <branch-name>
   ```
   Verify the new active branch with `git branch --show-current`.

---

### 2. Incremental Execution & Immediate Commits

Do **NOT** implement the entire plan in a single giant sweep. For each phase or logical task:

```
[Phase N Task Execution]
          │
          ▼
 [Run Localized Tests & Lints] ──(Fails)──► [Fix Immediately]
          │ (Passes)
          ▼
[Stage ONLY Relevant Files] (`git add <file1> <file2>`)
          │
          ▼
[Commit Meaningful Unit] (`git commit -m "<type>(<scope>): <summary>"`)
          │
          ▼
[Update Plan Checklist `[x]` & Commit]
          │
          ▼
(Advance to Phase N+1)
```

1. **Implement Minimal Atomic Unit**:
   - Strictly follow Test-Driven Development (TDD) where applicable: reproduction test $\to$ verify failure $\to$ implement fix $\to$ verify pass.
   - Keep the scope bounded strictly to the active task.

2. **Verify Integrity**:
   ```bash
   cargo check --all-targets
   cargo test --test <relevant_test>
   ```
   Ensure the codebase is in a green, working state before committing.

3. **Stage and Commit Immediately**:
   - Stage **only** the files modified for this specific unit:
     ```bash
     git add <file1> <file2> ...
     ```
   - Compose a Conventional Commit message matching repository conventions:
     - `feat(<scope>): <description>` for new capabilities
     - `fix(<scope>): <description>` for bug fixes
     - `test(<scope>): <description>` for new test suites
     - `refactor(<scope>): <description>` for internal restructuring
   - Commit immediately:
     ```bash
     git commit -m "<type>(<scope>): <imperative summary>" -m "- <bullet details>"
     ```

4. **Update Plan Progress**:
   - Update the plan checklist item from `- [ ]` to `- [x]`.
   - Stage and commit the plan update:
     ```bash
     git add plans/<plan-file>.md
     git commit -m "docs(plan): complete <task/phase name>"
     ```

---

### 3. Milestone Quality Gates

At the conclusion of major phases or when all plan tasks are complete:

```bash
# Full test suite
cargo test --all-targets

# Linter and formatting check
cargo clippy --all-targets -- -D warnings
cargo fmt --check

# Package/ecosystem checks if applicable
node scripts/test-npm-packages.js
```

If formatting or linter warnings arise, resolve them immediately in a dedicated `style` or `chore` commit:
```bash
git commit -m "style: fix clippy warnings and format code"
```

---

### 4. Final Review & Presentation

Once all plan checklist items are completed:

1. **Verify Clean Tree**:
   ```bash
   git status
   ```

2. **Review Branch History**:
   ```bash
   git log --oneline main..<branch-name>
   ```

3. **Present to User**:
   - Report the created branch name and full list of incremental commits.
   - Summarize verified test results.
   - Prompt the user whether they would like to push the branch (`git push -u origin <branch-name>`) or open a Pull Request.
