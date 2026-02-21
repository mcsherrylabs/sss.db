---
status: complete
priority: p1
issue_id: 001
tags: [code-review, documentation, accuracy, security]
dependencies: []
---

# Problem Statement

The CLAUDE.md blob handling example (lines 72-78) shows a `.tx` method that does not exist in the codebase. This creates a critical documentation accuracy issue that will confuse users and lead to compilation errors.

**Why it matters:** This is the recommended pattern for blob handling, so incorrect documentation directly impacts user code quality and prevents proper blob extraction.

## Findings

**Pattern Recognition Specialist:**
- Identified `.tx` method as non-existent in codebase
- No matches found for `def tx(` or `.tx(` patterns anywhere

**Repository Verification:**
- Verified across entire codebase: no `.tx` method exists on Table, View, or related classes
- Actual pattern uses for-comprehension with FutureTx monad

**Current Documentation (Lines 72-78):**
```scala
table.tx {
  val found = table.find(where(ps"blobVal = $bytes"))
  val bytes = found.get.blobByteArray("blobVal") // MUST be inside tx
}
```

**Actual Pattern from Tests (ForComprehensionSpec.scala:116-122):**
```scala
val plan = for {
  _ <- table.persist(Map("blobVal" -> testStr.getBytes))
  found <- table.find(where("blobVal = ?", bytes))
  extractedBytes = found.get[Array[Byte]]("blobVal") // Extract inside FutureTx
} yield extractedBytes
plan.runSyncAndGet
```

## Proposed Solutions

### Option 1: Replace with Correct For-Comprehension Pattern (Recommended)
**Implementation:**
```scala
**Blob Handling**: When working with byte arrays/blobs, extraction must happen INSIDE the transaction:
```scala
val blobData = (for {
  found <- table.find(where(ps"blobVal = $bytes"))
  data = found.get.blobByteArray("blobVal") // Extract inside FutureTx
} yield data).runSyncAndGet
```

**Pros:**
- Matches actual codebase patterns
- Compiles and works correctly
- Shows proper FutureTx usage

**Cons:**
- Slightly more verbose than fictional `.tx`

**Effort:** Small (5 minutes)
**Risk:** None - purely documentation fix

### Option 2: Add Utility Method `.tx` to Table Class
**Implementation:** Add actual `.tx` method to Table class for convenience.

**Pros:**
- Makes documentation retroactively correct
- Provides simpler API

**Cons:**
- Requires code changes, not just docs
- Adds API surface area
- Not necessary (for-comprehension works fine)

**Effort:** Medium (1 hour - implementation + tests)
**Risk:** Medium - API changes require testing

### Option 3: Remove Example Entirely
**Implementation:** Delete blob handling section.

**Pros:**
- Quick fix

**Cons:**
- Loses important guidance about blob extraction timing

**Effort:** Small (2 minutes)
**Risk:** Low but removes valuable information

## Recommended Action

**Option 1** - Replace with correct for-comprehension pattern.

This maintains the critical guidance about blob extraction timing while using accurate, working code.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (lines 72-78)

**Components:**
- Documentation

**Database Changes:** None

## Acceptance Criteria

- [ ] Blob handling example uses valid Scala code that compiles
- [ ] Example demonstrates extracting blob data inside FutureTx monad
- [ ] Example shows proper execution with `.runSyncAndGet`
- [ ] No references to non-existent `.tx` method remain

## Work Log

**2026-01-28:** Issue identified during code review by pattern-recognition-specialist and repo-research-analyst agents. All agents confirmed `.tx` method does not exist.

## Resources

- **Related Files:**
  - `/home/alan/develop/sss.db/src/test/scala/sss/db/ForComprehensionSpec.scala` (correct pattern examples)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/Table.scala` (actual Table API)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (Row.blobByteArray method)

- **Similar Patterns:**
  - All test files use for-comprehension pattern consistently
  - No `.tx` convenience method exists anywhere in codebase
