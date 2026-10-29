# Issue Tracker: GitHub

Operational guide for managing repository issues, tickets, and wayfinding maps using the `gh` CLI.

## Core issue operations

- **Create**: `gh issue create --title "..." --body "..."` (use heredoc or standard stdin for multi-line bodies).
- **Read**: `gh issue view <number> --json number,title,body,labels,comments`
- **List**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` (filter via `--label` and `--state`).
- **Discover child tickets**: `gh issue list --state all --search "Part of #<spec>" --json number,title,labels`
- **Comment**: `gh issue comment <number> --body "..."`
- **Label**: `gh issue edit <number> --add-label "..."` / `gh issue edit <number> --remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

## Triage surface policy

- **PRs as a request surface**: `no`. All bug reports, feature requests, and work tickets originate strictly as GitHub issues.
- **Number space ambiguity**: GitHub unifies issue and PR numbering; if `#<n>` is ambiguous, inspect via `gh issue view <n>` first, falling back to `gh pr view <n>`.

## Wayfinding operations

Map and ticket workflows for `/wayfinder`:

- **Map**: Single tracking issue labelled `wayfinder:map` housing `Notes`, `Decisions-so-far`, and `Fog` sections (`gh issue create --label wayfinder:map`).
- **Child ticket**: Task issue linked to the parent map as a sub-issue or referenced via `Part of #<map>` at the top of the body. Labels: `wayfinder:<type>` (`research` | `prototype` | `grilling` | `task`).
- **Dependencies**: Native issue blocking via `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>` (using the numeric database id from `gh api repos/<owner>/<repo>/issues/<n> --jq .id`). Fallback: `Blocked by: #<n>` in child body.
- **Frontier query**: Filter map's open children with zero open blockers (`issue_dependencies_summary.blocked_by == 0`) and no assignee (`assignees == []`). First in map order is the active frontier.
- **Claim**: `gh issue edit <n> --add-assignee "@me"` before executing work.
- **Resolve**: Add summary comment, close ticket (`gh issue close <n> --comment "..."`), and append a context pointer (gist + link) to the map's `Decisions-so-far`.
