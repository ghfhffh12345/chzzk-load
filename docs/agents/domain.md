# Domain Documentation

Reference guide for consuming `chzzk-load` domain models and architectural decisions.

## Ground in the domain

Before planning, refactoring, or naming components:

1. **Consult `GLOSSARY.md`**: Ground all concepts in established repository terminology. Use the defined terms in struct names, module interfaces, test cases, and commit messages; reject synonyms listed under `_Avoid_`.
2. **Review `docs/adr/`**: Read the ADRs governing the subsystem you touch (e.g., `0001` for lifecycle state, `0003` for upload DLQ, `0008` for custodian directory lifecycle, `0010`–`0012` for consolidation and naming, `0013` for VOD pipeline).

## Domain boundaries

`chzzk-load` operates as a cohesive single-context architecture:

- Root glossary: `GLOSSARY.md`
- Architectural decisions: `docs/adr/0001-*.md` through `0013-*.md`

If a concept is absent from `GLOSSARY.md`, note the gap for `/domain-modeling` rather than introducing unvetted synonyms.

## Flag ADR conflicts

Surface any divergence from an existing ADR explicitly rather than overriding it silently:

> _Contradicts ADR-0010 (Concat Demuxer Timestamp Normalization), but worth reopening because…_
