# Agent skills used to build this project

Every skill the agent (Claude Code) used while building this repository, copied here so the process
is reviewable and reproducible. The list was taken from the session transcripts: skills that were
invoked, plus the skill files and scripts the agent read or executed.

| Phase | Skill | Source | How it was used |
|---|---|---|---|
| All | [`using-superpowers`](using-superpowers/SKILL.md) | superpowers 7.0.2 | Session bootstrap: check for an applicable skill before acting |
| Analysis & design | [`brainstorming`](brainstorming/SKILL.md) | superpowers 7.0.2 | Requirements questions, approaches, the [design spec](../../docs/superpowers/specs/), builder check of the spec |
| Design | [`clean-architecture`](clean-architecture/SKILL.md) | project | Layering and dependency rule; enforced by import-linter |
| Design | [`fastapi`](fastapi/SKILL.md) | FastAPI official skill | `Annotated` dependencies, return types, routers |
| Design | [`python-patterns`](python-patterns/SKILL.md) | everything-claude-code 2.2.3 | Typing, `StrEnum`, frozen dataclasses, error chaining |
| Design | [`deployment-patterns`](deployment-patterns/SKILL.md) | everything-claude-code 2.2.3 | Dockerfile, compose, health checks, CI plan |
| Planning | [`writing-plans`](writing-plans/SKILL.md) | superpowers 7.0.2 | The three [implementation plans](../../docs/superpowers/plans/) |
| Execution | [`executing-plans`](executing-plans/SKILL.md) | superpowers 7.0.2 | Inline task-by-task execution with a ledger of rulings (`scripts/task-start`, `scripts/task-done`) |
| Execution | [`subagent-driven-development`](subagent-driven-development/SKILL.md) | superpowers 7.0.2 | Its `scripts/sdd-workspace` and `scripts/review-package`, used by `executing-plans` |
| Execution | [`test-driven-development`](test-driven-development/SKILL.md) | superpowers 7.0.2 | RED → GREEN for every task and every review fix |
| Execution | [`using-git-worktrees`](using-git-worktrees/SKILL.md) | superpowers 7.0.2 | Workspace setup (work stayed on `main` at the requester's choice) |
| Execution | [`outbox-checklist`](outbox-checklist/SKILL.md) | project | Written for this repo: checklist for any event, queue or consumer change |
| Review | [`requesting-code-review`](requesting-code-review/SKILL.md) | superpowers 7.0.2 | `code-reviewer.md` prompt for the final whole-branch review of each plan |
| Finish | [`finishing-a-development-branch`](finishing-a-development-branch/SKILL.md) | superpowers 7.0.2 | Verify, then present integration options (push only with explicit approval) |

Project rules that apply to every agent session are in [`CLAUDE.md`](../../CLAUDE.md).

## Third-party skills

Third-party skills are copied as they were used, at the versions listed above:

- superpowers — <https://github.com/obra/superpowers>, MIT, [license](LICENSES/superpowers.txt)
- everything-claude-code (ECC) — <https://github.com/affaan-m/ECC>, MIT,
  [license](LICENSES/everything-claude-code.txt)
- fastapi — FastAPI's official agent skill, <https://github.com/fastapi/fastapi>, MIT

If the superpowers plugin is also installed, these copies appear as project skills alongside the
plugin's `superpowers:*` versions. They have the same content.
