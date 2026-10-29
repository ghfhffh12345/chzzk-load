# Triage Labels

Mapping between canonical triage roles and the label strings configured in this repository's issue tracker.

| Canonical Role     | Tracker Label      | Meaning                                  |
| ------------------ | ------------------ | ---------------------------------------- |
| `needs-triage`     | `needs-triage`     | Maintainer evaluation required           |
| `needs-info`       | `needs-info`       | Blocked awaiting reporter feedback       |
| `ready-for-agent`  | `ready-for-agent`  | Fully specified for automated execution |
| `ready-for-human`  | `ready-for-human`  | Requires manual human implementation     |
| `wontfix`          | `wontfix`          | Closed as out-of-scope or will not fix   |

When a skill or workflow specifies a triage role (e.g. "apply the AFK-ready triage label"), apply the corresponding `Tracker Label` string.
