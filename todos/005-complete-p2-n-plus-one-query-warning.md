---
status: complete
priority: p2
issue_id: 005
tags: [code-review, performance, documentation]
dependencies: []
---

# Problem Statement

CLAUDE.md shows a for-comprehension example (lines 39-42) without warning about N+1 query patterns. Users unfamiliar with database performance could easily create severe performance issues by calling database operations in loops.

**Why it matters:** N+1 queries are a common performance anti-pattern that causes exponential slowdown at scale. 100 related records become 101 queries instead of 2, causing connection pool exhaustion and timeout issues.

## Findings

**Performance Oracle:**
- Documentation shows composable operations but no N+1 warning
- No guidance on batch operations
- Missing WHERE IN clause examples
- No mention of FutureTx.sequence for bulk operations

**Projected Impact:**
- 10 rows: ~100ms (negligible)
- 100 rows: ~1 second (noticeable)
- 1,000 rows: ~10 seconds (unacceptable)
- Connection pool exhaustion under load

## Proposed Solutions

### Option 1: Add Performance Best Practices Section (Recommended)
**Implementation:**
Add new section after Configuration:

```markdown
## Performance Best Practices

### N+1 Query Prevention

Never call database operations inside a loop over query results. This creates N+1 queries, causing severe performance degradation.

**Anti-pattern (N+1 queries):**
```scala
// Fetches each related row individually
val users = userTable.findAll().runSyncAndGet
users.map { user =>
  val orders = orderTable.find(where(ps"user_id = ${user.id}")).runSyncAndGet
  (user, orders) // Creates 1 + N queries
}
```

**Good pattern (2 queries):**
```scala
val users = userTable.findAll().runSyncAndGet
val userIds = users.map(_.id)
val orders = orderTable.filter(where(ps"user_id").in(userIds)).runSyncAndGet
val ordersByUser = orders.groupBy(_.long("user_id"))
users.map(user => (user, ordersByUser.getOrElse(user.id, Seq.empty)))
```

### Batch Operations

Use `FutureTx.sequence` to batch independent operations in a single transaction:

**Anti-pattern (N transactions):**
```scala
data.foreach { item =>
  table.insert(item).runSyncAndGet // Separate transaction per insert
}
```

**Good pattern (1 transaction):**
```scala
val inserts = data.map(table.insert)
val batchOp = FutureTx.sequence(inserts)
batchOp.runSyncAndGet // Single transaction for all inserts
```

### Large Result Sets

Use PagedView for queries returning >10,000 rows:
```scala
table.toPaged(pageSize = 1000).toIterator.grouped(1000).foreach { batch =>
  processBatch(batch) // Only 1000 rows in memory at a time
}
```

### Query Optimization
- Always use WHERE clauses with indexed columns
- Avoid SELECT * on tables with many columns or blobs
- Use specific column lists: `new View("table", where(), runContext, freeBlobsEarly, "id,name")`
- Leverage prepared statement caching (enabled by default)
```

**Pros:**
- Comprehensive performance guidance
- Shows both anti-patterns and solutions
- Covers N+1, batching, pagination
- Actionable examples

**Cons:**
- Adds ~50 lines to documentation

**Effort:** Medium (25 minutes)
**Risk:** None - purely additive

### Option 2: Add Warning to For-Comprehension Example
**Implementation:** Add inline note after the example.

**Pros:**
- Contextual to where problem appears
- Minimal documentation growth

**Cons:**
- Less comprehensive
- Doesn't cover batch operations

**Effort:** Small (5 minutes)
**Risk:** Low but incomplete

### Option 3: Create Separate Performance Guide
**Implementation:** New performance.md file.

**Pros:**
- Detailed coverage possible
- Keeps CLAUDE.md focused

**Cons:**
- Discoverability issues
- Users may not find it

**Effort:** Medium (40 minutes)
**Risk:** Medium - discoverability

## Recommended Action

**Option 1** - Add Performance Best Practices section to CLAUDE.md.

This provides essential performance guidance where users will see it during initial learning.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (add Performance Best Practices section)

**Components:**
- Documentation

**Database Changes:** None

**Performance Characteristics:**
```
N+1 Pattern (100 records):
- Queries: 101 (1 parent + 100 children)
- Time: ~1000ms (10ms per query)
- Connections: 101 sequential acquisitions

WHERE IN Pattern (100 records):
- Queries: 2 (1 parent + 1 children batch)
- Time: ~20ms (10ms per query)
- Connections: 2 sequential acquisitions

Performance improvement: 50x faster
```

## Acceptance Criteria

- [ ] Performance Best Practices section exists
- [ ] N+1 query anti-pattern documented with examples
- [ ] Batch operation pattern with FutureTx.sequence shown
- [ ] WHERE IN clause usage demonstrated
- [ ] PagedView usage for large result sets documented
- [ ] Query optimization tips included

## Work Log

**2026-01-28:** Issue identified by performance-oracle agent. Projected severe performance degradation from undocumented N+1 patterns.

## Resources

- **Related Patterns:**
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (Where.in method at line 239)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/FutureTx.scala` (sequence method for batching)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/PagedView.scala` (pagination support)
