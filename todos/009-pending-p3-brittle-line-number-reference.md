---
status: pending
priority: p3
issue_id: 009
tags: [code-review, documentation, maintenance]
dependencies: []
---

# Problem Statement

CLAUDE.md line 128 contains a brittle reference: "PGP key configured (see build.sbt line 52)". This line number reference will become incorrect whenever build.sbt is modified above line 52, causing documentation to point to the wrong code.

**Why it matters:** Line number references break easily. Users following the reference will be confused when it points to unrelated code. This is a documentation anti-pattern.

## Findings

**Pattern Recognition Specialist:**
- Identified "Stale Reference Pattern"
- Line number will become incorrect with any build.sbt edits
- Better to reference by setting name or section

**Current Documentation (Line 128):**
```markdown
Published to Maven Central via Sonatype. Publishing requires:
- PGP key configured (see build.sbt line 52)
- SONA_USER and SONA_PASS environment variables
- Tag push triggers GitHub Actions CI/CD workflow
```

**Actual build.sbt:**
Line 52 currently contains PGP configuration, but this will drift.

## Proposed Solutions

### Option 1: Reference by Setting Name (Recommended)
**Implementation:**
```markdown
Published to Maven Central via Sonatype. Publishing requires:
- PGP key configured (see `usePgpKeyHex` setting in build.sbt)
- SONA_USER and SONA_PASS environment variables
- Tag push triggers GitHub Actions CI/CD workflow
```

**Pros:**
- Stable reference (setting name won't change)
- Easy to search for in build.sbt
- More meaningful than line number

**Cons:**
- None

**Effort:** Trivial (1 minute)
**Risk:** None

### Option 2: Remove Reference Entirely
**Implementation:**
```markdown
Published to Maven Central via Sonatype. Publishing requires:
- PGP key configured in build.sbt
- SONA_USER and SONA_PASS environment variables
- Tag push triggers GitHub Actions CI/CD workflow
```

**Pros:**
- Simplest solution
- Can't go stale

**Cons:**
- Less specific
- User has to search build.sbt

**Effort:** Trivial (1 minute)
**Risk:** None

### Option 3: Reference by Section Comment
**Implementation:**
Add comment to build.sbt, reference in docs:
```scala
// PGP signing configuration
usePgpKeyHex("...")
```

```markdown
- PGP key configured (see PGP signing section in build.sbt)
```

**Pros:**
- Very stable
- Self-documenting build file

**Cons:**
- Requires changing build.sbt
- More work

**Effort:** Small (5 minutes)
**Risk:** None

## Recommended Action

**Option 1** - Reference by setting name (`usePgpKeyHex`).

This is stable, specific, and requires minimal effort.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (line 128)

**Components:**
- Documentation

**Database Changes:** None

## Acceptance Criteria

- [ ] Line number reference removed from CLAUDE.md
- [ ] Replaced with setting name or section reference
- [ ] Reference is stable and won't become incorrect
- [ ] Users can easily find the referenced configuration

## Work Log

**2026-01-28:** Issue identified by pattern-recognition-specialist agent as "Stale Reference Pattern" anti-pattern.

## Resources

- **Documentation Anti-Patterns:**
  - Line number references (brittle)
  - "See above/below" (layout-dependent)
  - Dated references ("as of version X")

- **Better Patterns:**
  - Named references (function names, setting names)
  - Section references
  - Semantic anchors
