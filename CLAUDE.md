# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

sss.db is a Scala library for basic SQL database access with a focus on simplicity and type-safety. It provides:
- Transaction support with FutureTx monad for composable database operations
- Optimistic versioning (automatic when table has 'version' column)
- Connection pooling (HikariCP)
- Natural DSL for queries using prepared statements
- Both synchronous and asynchronous execution contexts

## Build Commands

```bash
# Compile and test
sbt clean test

# Run specific test
sbt "testOnly sss.db.DbSpec"

# Publish to Maven Central (requires credentials)
sbt publishSigned
```

## Architecture

### Core Abstractions

**FutureTx[T]**: The central abstraction - a function `TransactionContext => Future[T]` that represents a database operation within a transaction. It's a monad supporting flatMap/map for composing operations:
```scala
val composedOp: FutureTx[Row] = for {
  row1 <- table1.persist(Map("col" -> "value"))
  row2 <- table2.persist(Map("ref" -> row1.id))
} yield row2
```

**RunContext**: Two implementations control execution:
- `SyncRunContext`: Blocking execution via `.runSync`/`.runSyncAndGet`
- `AsyncRunContext`: Async execution via `.run` returning `Future[T]`

**Db**: Entry point that creates Tables, Views, and manages database lifecycle. Created with `Db(configName)` or `Db(config)(dataSource, executionContext)`.

### Key Classes

- **Table**: Full read/write access to a table. Extends InsertableView.
- **View**: Read-only database view with count/max operations.
- **UpdatableView**: View with update/delete capabilities.
- **InsertableView**: View with insert capabilities (persist/insert methods).
- **Query**: Base class for executing SELECT queries with DSL support.
- **Where**: Builder for WHERE clauses with method chaining for orderBy/limit/and.
- **Row**: Result row with type-safe accessors (`.string()`, `.long()`, `.int()`, etc.).

### Important Patterns

**Transaction Execution**: All database operations are FutureTx values executed by calling:
- `.runSync` (synchronous, returns Try[T])
- `.run` (asynchronous, returns Future[T])

**Prepared Statements**: Use the `ps` string interpolator for safe parameterized queries:
```scala
where(ps"column = $value AND other = $otherValue")
```

**Blob Handling**: When working with byte arrays/blobs, extraction must happen INSIDE the transaction:
```scala
val blobData = (for {
  found <- table.find(where(ps"blobVal = $bytes"))
  data = found.get.blobByteArray("blobVal") // Extract inside FutureTx
} yield data).runSyncAndGet
```

**Optimistic Locking**: Automatic if table definition includes a 'version' column. Updates increment version and fail if version changed.

### Error Handling

Database operations return `Try[T]` (sync) or `Future[T]` (async). Transactions automatically rollback on exception and close connections.

**Exception Types:**
- `DbException`: Recoverable errors (constraint violations, deadlocks, timeouts)
- `DbOptimisticLockingException`: Version conflict during update (subclass of DbException)
- `DbError`: Unrecoverable errors (configuration issues, schema problems)

**Synchronous Error Handling:**
```scala
table.persist(values).runSync match {
  case Success(row) =>
    println(s"Created row ${row.id}")
  case Failure(e: DbOptimisticLockingException) =>
    // Retry with fresh version
    retryOperation()
  case Failure(e: DbException) =>
    logger.error(s"Database error: ${e.getMessage}")
  case Failure(e) =>
    throw e
}
```

**Asynchronous Error Handling:**
```scala
table.persist(values).run.map { row =>
  println(s"Created row ${row.id}")
}.recover {
  case e: DbOptimisticLockingException => // Retry logic
  case e: DbException => // Handle error
}
```

**Retry Pattern for Optimistic Locking:**
```scala
def persistWithRetry[T](op: FutureTx[T], maxRetries: Int = 3): Try[T] = {
  (1 to maxRetries).iterator.map { attempt =>
    op.runSync match {
      case s @ Success(_) => return s
      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        Thread.sleep(100 * attempt) // Exponential backoff
      case f @ Failure(_) => return f
    }
  }.toSeq
  Failure(new Exception("Max retries exceeded"))
}
```

### Transaction Semantics

**FutureTx operations are:**
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

**Transaction timeout:** Default is 3 seconds for SyncRunContext. Override for long operations:
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

**Pattern:** Library uses try/finally for resource cleanup (not Try monad) to ensure exceptions propagate while guaranteeing cleanup.

### Security Considerations

**SQL Injection Prevention**: This library uses prepared statements for **values**, which prevents SQL injection. However, table names, column names, and SQL keywords **cannot be parameterized**.

**Safe (parameterized values):**
```scala
where(ps"email = $userInput AND status = $statusInput")  // ✓ SAFE
```

**Unsafe (dynamic identifiers):**
```scala
where(s"$userColumn = ?", value)  // ✗ VULNERABLE if userColumn is untrusted
```

**For dynamic column names, use validation:**
```scala
val allowedColumns = Set("id", "email", "status", "created_at")
require(allowedColumns.contains(columnName), s"Invalid column: $columnName")
where(s"$columnName = ?", value) // Now safe
```

**Dangerous Operations**: The `executeSql` method bypasses prepared statements and should be used with extreme caution. Only use for trusted SQL (migrations, admin operations), never with user input.

## Configuration

⚠️ **SECURITY WARNING:** Never commit database credentials to version control. Use environment variables for production credentials.

### Production Configuration

Use environment variables to avoid hardcoding credentials:

```
database {
  datasource {
    driver = "org.postgresql.Driver"
    connection = ${DATABASE_URL}      # From environment variable
    user = ${DATABASE_USER}           # From environment variable
    pass = ${DATABASE_PASSWORD}       # From environment variable
    maxPoolSize = 10
  }

  viewCachesSize = 100
  useShutdownHook = false
  freeBlobsEarly = false
}
```

**Environment variable syntax:**
- `${VAR_NAME}` - Required, fails if not set
- `${?VAR_NAME}` - Optional, empty if not set

### Test Configuration

For tests only, inline credentials are acceptable:

```
testDb {
  datasource {
    driver = "org.hsqldb.jdbc.JDBCDriver"
    connection = "jdbc:hsqldb:mem:test"
    user = "SA"
    pass = ""
    maxPoolSize = 10
  }

  viewCachesSize = 100
  useShutdownHook = false
  freeBlobsEarly = false

  # Optional: SQL to run on startup
  deleteSql = ["DROP TABLE IF EXISTS old_table"]
  createSql = ["CREATE TABLE IF NOT EXISTS my_table (...)"]
}
```

### Configuration Reference

**Core Settings:**

- **viewCachesSize** (default: 100): Cache size for View metadata. Caches column metadata per View to avoid repeated DB calls. Increase for apps with many distinct views (>100 unique queries). Each cache entry is small (~1KB), safe to increase to 1000+.

- **useShutdownHook** (default: true): Register JVM shutdown hook to close connections. Set false if managing lifecycle manually or using container shutdown hooks.

- **freeBlobsEarly** (default: false): Release blob memory immediately after extraction. Set true to free memory sooner at cost of small performance overhead. Recommended for large blobs or high memory pressure.

- **deleteSql** / **createSql**: Optional SQL statements to run on database startup. Useful for setup/teardown in tests or creating tables on first run.

**Connection Pool Tuning (HikariCP):**

```
datasource {
  # Pool sizing
  maxPoolSize = 10                    # Max concurrent connections
  minimumIdle = 2                     # Min idle connections maintained

  # Timeouts (milliseconds)
  connectionTimeout = 30000           # Max wait for connection (30s)
  idleTimeout = 600000                # Idle connection lifetime (10m)
  maxLifetime = 1800000               # Max connection lifetime (30m)

  # Prepared statement caching
  cachePrepStmts = true               # Enable caching
  prepStmtCacheSize = 250             # Cache up to 250 statements
  prepStmtCacheSqlLimit = 2048        # Cache statements up to 2KB

  # Performance tuning
  useServerPrepStmts = true           # Use server-side prep statements
}
```

**Connection pool sizing guidelines:**
- Sync contexts: pool size ≈ max concurrent blocking threads
- Async contexts: smaller pool OK (futures queue efficiently)
- Formula: max_concurrent_transactions + 2-5 buffer
- Monitor: connection wait times, active connections

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
- Leverage prepared statement caching (enabled by default with HikariCP)

## Testing

Tests use ScalaTest. See test setup traits in `src/test/scala/sss/db/DbSpecSetup.scala` (DbSpecSetup, DbSpecQuickSetup, AsyncDbSpecSetup) and test examples in `src/test/scala/sss/db/DbSpec.scala`.

## Code Style Notes

- Uses implicit RunContext for execution (must be in scope for Tables/Views/Queries)
- Column names are case-insensitive (converted to lowercase)
