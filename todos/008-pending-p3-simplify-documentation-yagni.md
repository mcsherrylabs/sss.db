---
status: pending
priority: p3
issue_id: 008
tags: [code-review, documentation, simplicity]
dependencies: []
---

# Problem Statement

CLAUDE.md contains unnecessary complexity and YAGNI violations. The file includes generic sbt commands, stale version numbers, publishing details for contributors (not AI users), and verbose explanations that AI doesn't need. Document could be 24% shorter (138 → ~90 lines) without losing essential information.

**Why it matters:** Shorter, focused documentation is easier to scan and maintain. Over-explanation wastes token budget and reduces signal-to-noise ratio.

## Findings

**Code Simplicity Reviewer:**
- Generic sbt commands (lines 16-31) aren't project-specific
- Version numbers (lines 115-123) will go stale
- Publishing section (lines 125-130) is contributor info, not usage
- Code Style Notes (lines 132-138) mostly discoverable
- Meta-explanation (line 3) is redundant

**Estimated LOC reduction: 33 lines (24%)**

**YAGNI Violations:**
1. Publishing section - AI doesn't publish packages
2. Build commands - generic sbt knowledge
3. Scala/Java versions - hardcoded versions go stale
4. Testing framework trait names - discoverable

## Proposed Solutions

### Option 1: Major Simplification (Recommended)
**Implementation:**
Remove or drastically reduce:

1. **Delete lines 125-130 (Publishing)** - Pure YAGNI
2. **Delete lines 115-123 (Versions)** - Will go stale, AI can read build.sbt
3. **Reduce lines 16-31 (Build Commands)** to:
```bash
# Compile and test
sbt clean test

# Publish to Maven Central (requires credentials)
sbt publishSigned
```
4. **Reduce lines 106-113 (Testing)** to:
```markdown
## Testing
Tests use ScalaTest. See `DbSpec` and test setup traits in `src/test/scala/sss/db/DbSpecSetup.scala`.
```
5. **Delete line 3** (meta-explanation)
6. **Reduce lines 134-138 (Code Style)** to:
```markdown
## Code Style Notes
- Uses implicit RunContext for execution (must be in scope)
- Column names are case-insensitive (converted to lowercase)
```

**Result:** 138 lines → ~90 lines (35% reduction)

**Pros:**
- Clearer, more scannable
- Focused on usage, not maintenance
- Won't go stale
- Better signal-to-noise ratio

**Cons:**
- Less comprehensive
- May need to reference build.sbt for versions

**Effort:** Small (20 minutes)
**Risk:** Low - removing non-essential info

### Option 2: Reorder for Quick Start
**Implementation:** Move most important info to top, less important to bottom.

New order:
1. Project Overview
2. Quick Start Example (NEW - 10 lines)
3. Core Abstractions
4. Important Patterns
5. Configuration
6. Testing (reduced)
7. Build Commands (reduced)

**Pros:**
- Better information hierarchy
- Quick start helps new users

**Cons:**
- Doesn't reduce length
- More reorganization work

**Effort:** Medium (30 minutes)
**Risk:** Low - structural change

### Option 3: Minimal Changes (Keep Current Structure)
**Implementation:** Only remove Publishing section and stale versions.

**Pros:**
- Safe, minimal changes
- Preserves existing structure

**Cons:**
- Misses major simplification opportunity
- Still verbose

**Effort:** Small (5 minutes)
**Risk:** None

## Recommended Action

**Combination of Option 1 + 2** - Major simplification AND reordering.

This provides the most value: shorter, clearer, better organized.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (simplify throughout)

**Components:**
- Documentation

**Database Changes:** None

**Sections to Remove/Reduce:**
- Publishing (delete entirely)
- Scala Version and Dependencies (delete or move to footnote)
- Build Commands (keep only project-specific)
- Testing (reduce to single reference line)
- Code Style Notes (keep only critical items)

## Acceptance Criteria

- [ ] Publishing section removed
- [ ] Scala/Java version numbers removed (let AI read build.sbt)
- [ ] Build commands reduced to project-specific only
- [ ] Testing section reduced to single paragraph
- [ ] Meta-explanation line deleted
- [ ] Code Style Notes reduced to essential 2-3 items
- [ ] Total line count reduced by 25-35%
- [ ] All essential information retained

## Work Log

**2026-01-28:** Issue identified by code-simplicity-reviewer agent. Found 24% of documentation is unnecessary for AI usage.

## Resources

- **Complexity Assessment:**
  - Current: 138 lines
  - Essential info: ~90 lines
  - YAGNI content: ~48 lines (35%)

- **Related Principles:**
  - YAGNI (You Aren't Gonna Need It)
  - Signal-to-noise ratio optimization
  - Documentation maintenance burden
