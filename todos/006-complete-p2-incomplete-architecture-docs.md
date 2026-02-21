---
status: complete
priority: p2
issue_id: 006
tags: [code-review, architecture, documentation]
dependencies: []
---

# Problem Statement

CLAUDE.md Architecture section is incomplete. Missing critical architectural concerns: concurrency model, resource management details, transaction semantics (isolation levels), and architectural decision rationale. Users don't have enough information to build production-ready applications.

**Why it matters:** Without understanding thread safety, transaction semantics, and resource management, developers will make incorrect assumptions leading to race conditions, connection leaks, and data corruption.

## Findings

**Architecture Strategist:**
- Thread safety guarantees undocumented
- Transaction isolation levels missing (despite TxIsolationLevel enum existing)
- Resource management pattern mentioned but not explained
- No guidance on sync vs async context selection
- Missing ACID guarantees discussion
- No timeout configuration documentation

**Missing Documentation:**
1. **Concurrency model** - Are Table/View instances thread-safe?
2. **Transaction semantics** - Isolation levels, commit/rollback triggers, timeouts
3. **Resource management** - Connection lifecycle, cleanup guarantees
4. **Execution context selection** - When to use sync vs async, thread pool implications

## Proposed Solutions

### Option 1: Expand Architecture Section with Subsections (Recommended)
**Implementation:**
Add three new subsections to Architecture:

```markdown
### Transaction Semantics

FutureTx operations are:
- **Lazy**: No execution until `.run` or `.runSync` called
- **Atomic**: Either all operations commit or all rollback
- **Isolated**: Configurable isolation levels (default: READ_COMMITTED)
- **Composable**: Use for-comprehensions to combine operations

**Connection lifecycle:**
1. Acquired from pool when transaction starts
2. Auto-commit disabled
3. Operations executed within transaction
4. Commit on success (or rollback on exception)
5. Connection returned to pool (guaranteed via try/finally)

**Transaction isolation levels:**
```scala
import sss.db.TxIsolationLevel._

// Configure per-database
Db(config, dataSource, executionContext, SERIALIZABLE)
```

Available levels: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE

**Transaction timeout:**
Default is 3 seconds for SyncRunContext. Override for long operations:
```scala
implicit val customSync = new SyncRunContext(ec, timeout = 30.seconds)
```

### Concurrency and Thread Safety

**Thread-safe components:**
- `Table`/`View` instances - safe to share across threads
- `Row` instances - immutable, safe to pass between threads
- Connection pool - thread-safe

**Execution context selection:**

**SyncRunContext (blocking):**
- Blocks calling thread until operation completes
- Simpler mental model for sequential operations
- Use for: CLIs, simple scripts, test code
- Thread pool sizing: 1 thread per concurrent transaction
- Default timeout: 3 seconds

**AsyncRunContext (non-blocking):**
- Returns Future[T], doesn't block caller
- More efficient thread utilization
- Use for: Web services, high-concurrency apps
- Smaller connection pool acceptable (futures queue)
- Requires understanding of Future composition

**Rule of thumb:** If you need the result immediately and concurrency is low (<10 req/sec), use sync. For high throughput, use async.

### Resource Management

**Connection management:**
```scala
// Connections acquired on transaction start
// Automatically released on completion/error
val result = table.persist(data).runSync // Connection closed after this
```

**Cleanup guarantees:**
- PreparedStatements closed via try/finally
- ResultSets closed after Row extraction
- Connections returned to pool even on exception
- Blobs freed early when `freeBlobsEarly = true`

**Pattern (from View.scala:13-15):**
Library uses try/finally for resource cleanup (not Try monad) to ensure exceptions propagate while guaranteeing cleanup.
```

**Pros:**
- Complete architectural coverage
- Addresses all missing concerns
- Provides decision-making guidance

**Cons:**
- Adds ~80 lines to Architecture section

**Effort:** Medium (40 minutes)
**Risk:** None - purely additive documentation

### Option 2: Create Separate Architecture Guide
**Implementation:** New architecture.md file with deep dive.

**Pros:**
- Unlimited depth possible
- Keeps CLAUDE.md concise

**Cons:**
- Discoverability issues
- Critical info hidden from main doc

**Effort:** Large (2+ hours)
**Risk:** High - users won't find it

### Option 3: Add Brief Notes, Link to Source Code
**Implementation:** Add pointers to relevant source files.

**Pros:**
- Minimal documentation growth
- Code is source of truth

**Cons:**
- Requires reading source
- Not user-friendly

**Effort:** Small (10 minutes)
**Risk:** Medium - incomplete guidance

## Recommended Action

**Option 1** - Expand Architecture section with comprehensive subsections.

This provides essential architectural understanding without requiring separate documentation files.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (expand Architecture section)

**Components:**
- Documentation

**Database Changes:** None

**Referenced Code:**
- `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (TxIsolationLevel enum, line 70-77)
- `/home/alan/develop/sss.db/src/main/scala/sss/db/FutureTxExecutor.scala` (resource management)
- `/home/alan/develop/sss.db/src/main/scala/sss/db/Db.scala` (timeout configuration, line 48)
- `/home/alan/develop/sss.db/src/main/scala/sss/db/View.scala` (resource cleanup pattern, line 13-15)

## Acceptance Criteria

- [ ] Transaction Semantics subsection added to Architecture
- [ ] Documents isolation levels and how to configure them
- [ ] Explains transaction timeout and when to adjust
- [ ] Concurrency and Thread Safety subsection added
- [ ] Documents which components are thread-safe
- [ ] Provides sync vs async selection guidance with thread pool implications
- [ ] Resource Management subsection added
- [ ] Documents connection lifecycle and cleanup guarantees
- [ ] Explains try/finally pattern rationale

## Work Log

**2026-01-28:** Issue identified by architecture-strategist agent. Found TxIsolationLevel enum and timeout configuration in code but completely absent from documentation.

## Resources

- **Related Files:**
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (TxIsolationLevel)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/Db.scala` (timeout config)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/FutureTxExecutor.scala` (resource management)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/View.scala` (cleanup pattern comments)
