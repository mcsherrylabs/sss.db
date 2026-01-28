---
status: pending
priority: p2
issue_id: 004
tags: [code-review, architecture, documentation]
dependencies: []
---

# Problem Statement

CLAUDE.md lacks error handling documentation. The architecture section mentions optimistic locking "fails if version changed" (line 80) but doesn't explain exception types, error recovery patterns, or how to handle transaction failures. This leaves users without guidance for production-ready error handling.

**Why it matters:** Production applications must handle database errors gracefully. Without documentation, users will struggle with exception handling, retries, and transaction rollback patterns.

## Findings

**Architecture Strategist:**
- Error handling strategy completely undocumented
- No mention of DbException, DbError, DbOptimisticLockingException
- No guidance on Try/Future error patterns
- Missing retry strategies for transient failures

**Code Evidence (package.scala:126-132):**
```scala
class DbException(msg: String, val cause: Throwable) extends RuntimeException(msg, cause)
class DbError(msg: String, val cause: Throwable) extends Error(msg, cause)
class DbOptimisticLockingException(msg: String) extends Exception(msg)
```

**Execution Returns:**
- `.runSync` returns `Try[T]` (can be Success/Failure)
- `.run` returns `Future[T]` (can be completed/failed)
- No documentation on handling these

## Proposed Solutions

### Option 1: Add Error Handling Section to Architecture (Recommended)
**Implementation:**
Add to Architecture section after "Important Patterns":

```markdown
### Error Handling

Database operations return `Try[T]` (sync) or `Future[T]` (async). Transactions automatically rollback on exception and close connections.

**Exception Types:**
- `DbException`: Recoverable errors (constraint violations, deadlocks, timeouts)
- `DbOptimisticLockingException`: Version conflict during update (extends DbException)
- `DbError`: Unrecoverable errors (configuration issues, schema problems)

**Synchronous Error Handling:**
```scala
table.persist(values).runSync match {
  case Success(row) =>
    // Handle success
    println(s"Created row ${row.id}")
  case Failure(e: DbOptimisticLockingException) =>
    // Retry with fresh version
    retryOperation()
  case Failure(e: DbException) =>
    // Handle recoverable error
    logger.error(s"Database error: ${e.getMessage}")
  case Failure(e) =>
    // Unexpected error
    throw e
}
```

**Asynchronous Error Handling:**
```scala
table.persist(values).run.map { row =>
  println(s"Created row ${row.id}")
}.recover {
  case e: DbOptimisticLockingException =>
    // Retry logic
  case e: DbException =>
    // Handle error
}
```

**Retry Pattern for Optimistic Locking:**
```scala
def persistWithRetry[T](op: FutureTx[T], maxRetries: Int = 3): Try[T] = {
  (1 to maxRetries).toStream.map { attempt =>
    op.runSync match {
      case Success(result) => return Success(result)
      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        Thread.sleep(100 * attempt) // Exponential backoff
        // Continue to next attempt
      case Failure(e) => return Failure(e)
    }
  }
  Failure(new Exception("Max retries exceeded"))
}
```

**Transaction Rollback:**
All exceptions trigger automatic rollback. Connections are closed via try/finally even on error. No manual rollback needed.
```

**Pros:**
- Complete error handling guidance
- Shows all exception types
- Provides retry pattern for common case
- Documents automatic rollback

**Cons:**
- Adds ~50 lines to documentation

**Effort:** Medium (20 minutes)
**Risk:** None - purely additive

### Option 2: Add Error Handling Subsection in Important Patterns
**Implementation:** Brief error handling notes in existing section.

**Pros:**
- Minimal documentation growth
- Contextual to existing patterns

**Cons:**
- Less comprehensive
- May be overlooked

**Effort:** Small (10 minutes)
**Risk:** Low but incomplete

### Option 3: Create Separate Error Handling Guide
**Implementation:** New markdown file for error handling.

**Pros:**
- Detailed coverage possible
- Doesn't bloat CLAUDE.md

**Cons:**
- Users may not find it
- Requires navigation

**Effort:** Medium (30 minutes)
**Risk:** Medium - discoverability

## Recommended Action

**Option 1** - Add comprehensive Error Handling section to Architecture.

This provides complete guidance without requiring separate documentation files.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (add Error Handling section)

**Components:**
- Documentation

**Database Changes:** None

**Exception Hierarchy:**
```
Throwable
├── Error
│   └── DbError (unrecoverable)
└── Exception
    ├── DbException (recoverable)
    │   └── DbOptimisticLockingException (version conflict)
    └── ... (other exceptions)
```

## Acceptance Criteria

- [ ] Error Handling section exists in CLAUDE.md Architecture
- [ ] All three exception types documented with usage
- [ ] Shows Try pattern matching for .runSync
- [ ] Shows Future.recover for .run
- [ ] Provides retry pattern for optimistic locking
- [ ] Documents automatic rollback behavior
- [ ] Includes exponential backoff example

## Work Log

**2026-01-28:** Issue identified by architecture-strategist agent. Found three exception types in package.scala completely undocumented in CLAUDE.md.

## Resources

- **Related Files:**
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (exception definitions)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/FutureTxExecutor.scala` (rollback logic)
  - `/home/alan/develop/sss.db/src/test/scala/sss/db/ValidateTransactionSpec.scala` (rollback examples)
