# Performance

This guide covers performance optimization patterns for sss.db applications, focusing on preventing common anti-patterns and optimizing database access.

**Prerequisites:** Read [Queries](queries.md) and [Transactions](transactions.md) first.

## Table of Contents

- [N+1 Query Prevention](#n1-query-prevention)
- [Batch Operations](#batch-operations)  
- [Large Result Sets](#large-result-sets)
- [Query Optimization](#query-optimization)
- [Connection Pool Sizing](#connection-pool-sizing)
- [Concurrency Patterns](#concurrency-patterns)

---

## N+1 Query Prevention

The N+1 query problem is the most common performance issue in database applications.

### The Problem

```scala
// ❌ Anti-pattern: N+1 queries (1 + N)
val users = userTable.findAll().runSyncAndGet  // 1 query

users.foreach { user =>
  // N queries (one per user!)
  val posts = postTable.filter(
    where(ps"user_id = ${user.id}")
  ).runSyncAndGet

  println(s"${user.string("name")}: ${posts.length} posts")
}

// If you have 1000 users, this creates 1001 queries!
```

**Why it's bad:**
- Each query has network latency (~1-5ms)
- 1000 users = 1000 queries = 1-5 seconds just in network time
- Database connection held for entire duration
- Scales linearly with data size (terrible!)

### Solution 1: Batch Query with IN Clause

```scala
// ✅ Good: 2 queries total
val users = userTable.findAll().runSyncAndGet  // 1 query

// Fetch ALL posts in one query
val userIds = users.map(_.id)
val allPosts = postTable.filter(
  where("user_id").in(userIds)  
).runSyncAndGet  // 1 query

// Group posts by user_id
val postsByUser = allPosts.groupBy(_.long("user_id"))

// Associate posts with users (in-memory operation)
users.foreach { user =>
  val posts = postsByUser.getOrElse(user.id, Seq.empty)
  println(s"${user.string("name")}: ${posts.length} posts")
}

// Only 2 queries regardless of user count!
```

### Solution 2: JOIN at Application Level

```scala
// ✅ Good: Fetch related data together
case class UserWithPosts(user: Row, posts: Seq[Row])

val users = userTable.findAll().runSyncAndGet
val userIds = users.map(_.id)
val posts = postTable.filter(where("user_id").in(userIds)).runSyncAndGet

val userMap = users.map(u => u.id -> u).toMap
val postsByUser = posts.groupBy(_.long("user_id"))

val usersWithPosts = users.map { user =>
  UserWithPosts(
    user = user,
    posts = postsByUser.getOrElse(user.id, Seq.empty)
  )
}
```

### N+1 Detection

**Signs you have N+1 queries:**
- Loop over query results, querying inside loop
- Response time scales with data size  
- High query count in logs
- Connection pool exhaustion under load

---

## Batch Operations

### Batch Inserts

```scala
// ❌ Bad: N separate transactions
users.foreach { userData =>
  userTable.insert(userData).runSync  // Separate transaction per user
}

// ✅ Good: One transaction for all inserts
val insertOps = users.map(userTable.insert)
FutureTx.sequence(insertOps).runSync  // Single transaction
```

**Performance improvement:** 10-100x faster for large batches

### Chunked Batch Processing

For very large datasets:

```scala
def batchInsert(data: Seq[Map[String, Any]], chunkSize: Int = 1000): Unit = {
  data.grouped(chunkSize).zipWithIndex.foreach { case (chunk, index) =>
    val inserts = chunk.map(userTable.insert)
    FutureTx.sequence(inserts).runSyncAndGet
    println(s"Inserted chunk ${index + 1} (${chunk.length} rows)")
  }
}

// Process 100,000 users in chunks of 1000
batchInsert(largeDataset, chunkSize = 1000)
```

### Batch Updates

```scala
// ❌ Bad: N separate updates
userIds.foreach { id =>
  userTable.update(
    Map("active" -> true),
    where(ps"id = $id")
  ).runSync
}

// ✅ Good: Single IN clause update
userTable.update(
  Map("active" -> true),
  where("id").in(userIds)
).runSyncAndGet
```

---

## Large Result Sets

### PagedView for Streaming

For tables with >10,000 rows:

```scala
// ❌ Bad: Load everything into memory
val allUsers = userTable.findAll().runSyncAndGet  // Could be millions!

// ✅ Good: Stream with PagedView
val pagedView = userTable.toPaged(pageSize = 1000)

pagedView.toIterator.foreach { user =>
  processUser(user)
  // Only 1000 rows in memory at any time
}
```

### Pagination with offset/limit

```scala
def processInPages(pageSize: Int = 1000): Unit = {
  var page = 0
  var hasMore = true

  while (hasMore) {
    val users = userTable.filter(
      where()
      orderBy OrderBy("id")
      limit pageSize
      offset (page * pageSize)
    ).runSyncAndGet

    users.foreach(processUser)

    hasMore = users.length == pageSize
    page += 1
  }
}
```

### Specific Column Selection

Don't fetch columns you don't need:

```scala
// ❌ Bad: Fetches all columns (including large text fields, blobs)
val users = userTable.findAll().runSyncAndGet

// ✅ Good: Fetch only needed columns
val userView = new View(
  tableName = "users",
  where = Where(),
  runContext = db.syncRunContext,
  freeBlobsEarly = false,
  cols = "id, name, email"  // Only these columns
)

val users = userView.findAll().runSyncAndGet  // Much faster!
```

---

## Query Optimization

### Use Indexes

```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_created_at ON posts(created_at DESC);
```

```scala
// ✅ Fast (uses index on email)
userTable.filter(where(ps"email = $email")).runSyncAndGet

// ❌ Slow (can't use index - leading wildcard)
userTable.filter(where(ps"name LIKE ${s"%smith"}")).runSyncAndGet

// ✅ Fast (can use index - trailing wildcard)
userTable.filter(where(ps"name LIKE ${s"smith%"}")).runSyncAndGet
```

### Limit Result Sets

Always use LIMIT when you don't need all results:

```scala
// ✅ Good: Only fetch what you need
val recentPosts = postTable.filter(
  where()
  orderBy OrderDesc("created_at")
  limit 10
).runSyncAndGet
```

### Avoid SELECT *

See [Specific Column Selection](#specific-column-selection) above.

### Prepared Statement Caching

Enable in configuration:

```hocon
datasource {
  cachePrepStmts = true
  prepStmtCacheSize = 250
  prepStmtCacheSqlLimit = 2048
}
```

---

## Connection Pool Sizing

### Sizing Formulas

**SyncRunContext (blocking):**
```
maxPoolSize = expected_concurrent_transactions + buffer

Example:
- 50 concurrent API requests
- Each holds connection for ~100ms
- maxPoolSize = 50 + 5 (buffer) = 55
```

**AsyncRunContext (non-blocking):**
```
maxPoolSize = num_cpu_cores * 2

Example:
- 8-core machine
- maxPoolSize = 8 * 2 = 16
```

### Monitoring Pool Health

```scala
// Enable JMX metrics (HikariCP)
datasource {
  registerMbeans = true
}
```

**Monitor:**
- Active connections
- Idle connections  
- Connection wait time
- Connection acquisition time

**Alerts:**
- Wait time > 100ms → pool too small or slow queries
- Active connections near maxPoolSize → increase pool
- Many idle connections → decrease minimumIdle

### Pool Exhaustion Prevention

```scala
// ❌ Bad: Long transaction holds connection
val transaction = for {
  users <- userTable.findAll()
  _ = processUsers(users)  // Long computation!
  _ = sendEmails(users)    // Network I/O!
  results <- resultTable.persist(...)
} yield results

// ✅ Good: Short transaction
val users = userTable.findAll().runSyncAndGet
processUsers(users)  // Outside transaction
sendEmails(users)    // Outside transaction
resultTable.persist(...).runSync  // New short transaction
```

---

## Concurrency Patterns

### Thread-Safe Components

**Safe to share across threads:**
- `Table` / `View` instances
- `Row` instances (immutable)
- Connection pool

**Not thread-safe:**
- `RunContext` instances (but safe to have multiple)

```scala
// ✅ Good: Share table across threads
object Repository {
  val db = Db("database")
  val userTable = db.table("users")  // Shared safely
}

// Multiple threads can use Repository.userTable concurrently
```

### Sync vs Async Performance

**SyncRunContext:**
- Blocks calling thread
- Simple mental model
- Good for: CLI tools, batch jobs, low concurrency

**AsyncRunContext:**
- Non-blocking
- Better thread utilization
- Good for: Web services, high concurrency

**Throughput comparison:**

| Scenario | Sync | Async |
|----------|------|-------|
| 10 req/sec | ~equal | ~equal |
| 100 req/sec | Thread starvation | Good |
| 1000 req/sec | Not viable | Good |

### Concurrent Transactions

```scala
// Multiple independent transactions in parallel
val futures = (1 to 100).map { i =>
  Future {
    userTable.persist(Map("name" -> s"User $i")).runSync
  }
}

// Wait for all to complete
Await.result(Future.sequence(futures), 10.seconds)

// More efficient than sequential execution
```

---

## Performance Checklist

**Query Optimization:**
- [ ] No N+1 queries (use batching with IN clauses)
- [ ] Use indexes on WHERE columns
- [ ] Limit result sets when possible
- [ ] Fetch only needed columns
- [ ] Use PagedView for large result sets (>10k rows)

**Batch Operations:**
- [ ] Use FutureTx.sequence for batch inserts/updates
- [ ] Chunk very large batches (>1000 rows)
- [ ] Single transaction for related operations

**Connection Pool:**
- [ ] Sized appropriately for workload
- [ ] Monitor pool metrics (wait time, active connections)
- [ ] Keep transactions short
- [ ] Enable prepared statement caching

**Concurrency:**
- [ ] Use AsyncRunContext for high-concurrency apps
- [ ] Don't hold connections during long computations
- [ ] Share Table/View instances across threads

---

## Summary

**Top Performance Killers:**
1. N+1 queries (use IN clauses for batching)
2. Loading large result sets into memory (use PagedView)
3. Long-running transactions (keep them short)
4. Missing indexes (index WHERE columns)
5. SELECT * (fetch only needed columns)

**Quick Wins:**
1. Batch queries with IN clauses
2. Use FutureTx.sequence for batch operations
3. Enable prepared statement caching
4. Size connection pool correctly
5. Use PagedView for large tables

**Next Steps:**
- [Troubleshooting](troubleshooting.md) - Performance issues
- [Configuration](configuration.md) - Pool tuning
- [Queries](queries.md) - Query optimization
