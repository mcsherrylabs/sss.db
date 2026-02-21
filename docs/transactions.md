# Transactions

This guide provides a deep dive into transaction patterns and semantics in sss.db. You'll learn how to compose operations safely, handle errors properly, and understand the guarantees sss.db provides.

**Prerequisites:** Read [Core Concepts](core-concepts.md) first to understand FutureTx and RunContext.

## Table of Contents

- [Transaction Basics](#transaction-basics)
- [Composing Operations](#composing-operations)
- [Error Handling](#error-handling)
- [Transaction Isolation Levels](#transaction-isolation-levels)
- [Optimistic Locking](#optimistic-locking)
- [Connection Management](#connection-management)
- [Best Practices](#best-practices)

---

## Transaction Basics

### ACID Properties

sss.db transactions provide full ACID guarantees:

**Atomicity:** All operations in a transaction succeed together or fail together. There are no partial updates.

```scala
val transaction = for {
  user <- userTable.persist(Map("name" -> "Alice"))
  post <- postTable.persist(Map("user_id" -> user.id, "title" -> "Hello"))
  _ <- userTable.update(Map("post_count" -> 1), where("id" -> user.id))
} yield (user, post)

// Either all three operations succeed, or none do
transaction.runSync match {
  case Success(_) => println("All changes committed")
  case Failure(e) => println("All changes rolled back")
}
```

**Consistency:** Database constraints are enforced. If an operation violates a constraint (unique key, foreign key, check constraint), the entire transaction rolls back.

```scala
// If email is unique, this will fail and rollback
val transaction = for {
  user1 <- userTable.persist(Map("email" -> "alice@example.com"))
  user2 <- userTable.persist(Map("email" -> "alice@example.com"))  // Duplicate!
} yield (user1, user2)

transaction.runSync  // Failure - both inserts rolled back
```

**Isolation:** Transactions are isolated from each other. Configurable isolation levels control visibility of uncommitted changes (see [Transaction Isolation Levels](#transaction-isolation-levels)).

**Durability:** Once a transaction commits, changes are permanent (survive crashes, power loss, etc.). This is guaranteed by the underlying database.

### Lazy Execution

FutureTx operations are **lazy** - they build a plan but don't execute until you call `.run` or `.runSync`:

```scala
// Step 1: Build the operation (NO execution yet)
val operation = for {
  user <- userTable.persist(Map("name" -> "Bob"))
  post <- postTable.persist(Map("user_id" -> user.id))
} yield (user, post)

println("Operation created but not executed")

// Step 2: Execute the operation (NOW it runs)
val result = operation.runSync
println("Operation executed")
```

**Why laziness matters:**
- Build operations programmatically before executing
- Operations compose efficiently without intermediate execution
- Control exactly when database side effects occur
- Test operation logic without hitting the database

### Automatic Commit and Rollback

sss.db handles transaction lifecycle automatically:

```scala
val transaction = for {
  user <- userTable.persist(Map("name" -> "Charlie"))
  post <- postTable.persist(Map("user_id" -> user.id))
} yield (user, post)

transaction.runSync
// Behind the scenes:
// 1. Acquire connection from pool
// 2. Set autoCommit = false
// 3. Execute user insert
// 4. Execute post insert
// 5. COMMIT (if both succeed)
// 6. Close connection and return to pool
//
// If any step fails:
// 3a. Exception thrown
// 4a. ROLLBACK
// 5a. Close connection and return to pool
// 6a. Return Failure(exception)
```

**You never manually commit or rollback.** The library handles it based on success or failure.

### Connection Lifecycle

Understanding the connection lifecycle helps debug performance issues:

```
┌─────────────────────────────────────────────────────────┐
│ 1. transaction.runSync called                           │
├─────────────────────────────────────────────────────────┤
│ 2. Acquire connection from HikariCP pool                │
│    (blocks if pool exhausted, up to connectionTimeout)  │
├─────────────────────────────────────────────────────────┤
│ 3. Set autoCommit = false                               │
├─────────────────────────────────────────────────────────┤
│ 4. Set isolation level (if configured)                  │
├─────────────────────────────────────────────────────────┤
│ 5. Execute operations sequentially                      │
│    - Create PreparedStatements                          │
│    - Bind parameters                                    │
│    - Execute SQL                                        │
│    - Extract results                                    │
├─────────────────────────────────────────────────────────┤
│ 6. SUCCESS path:                                        │
│    - conn.commit()                                      │
│    - Return Success(result)                             │
│                                                          │
│    FAILURE path:                                        │
│    - conn.rollback()                                    │
│    - Return Failure(exception)                          │
├─────────────────────────────────────────────────────────┤
│ 7. Close PreparedStatements (try/finally)               │
├─────────────────────────────────────────────────────────┤
│ 8. Return connection to pool (try/finally)              │
│    (guaranteed even if exception thrown)                │
└─────────────────────────────────────────────────────────┘
```

**Key points:**
- Connections are scarce resources (limited by `maxPoolSize`)
- Long-running transactions hold connections, reducing availability
- Connections are **always** returned to pool (try/finally ensures this)
- If pool is exhausted, operations wait (up to `connectionTimeout`)

---

## Composing Operations

The power of FutureTx comes from composing multiple database operations into a single atomic transaction.

### For-Comprehensions

The most common way to compose operations:

```scala
val transaction = for {
  // Step 1: Create user
  user <- userTable.persist(Map(
    "name" -> "Alice",
    "email" -> "alice@example.com"
  ))

  // Step 2: Create profile (depends on user.id)
  profile <- profileTable.persist(Map(
    "user_id" -> user.id,
    "bio" -> "Software engineer"
  ))

  // Step 3: Create initial post (depends on user.id)
  post <- postTable.persist(Map(
    "user_id" -> user.id,
    "title" -> "Hello World",
    "content" -> "My first post!"
  ))

  // Step 4: Update user's post count
  _ <- userTable.update(
    Map("post_count" -> 1),
    where(ps"id = ${user.id}")
  )

} yield (user, profile, post)

// All four operations execute in a single transaction
transaction.runSync match {
  case Success((user, profile, post)) =>
    println(s"Created user ${user.id} with profile and post")

  case Failure(e) =>
    println(s"Transaction failed - all changes rolled back: ${e.getMessage}")
}
```

**What happens:**
- All operations execute in **one transaction**
- Each operation can access results from previous operations
- If any operation fails, **all changes rollback**
- Result type is `FutureTx[(Row, Row, Row)]`

### Sequential vs Parallel Execution

Operations in a for-comprehension execute **sequentially** (each waits for the previous to complete):

```scala
// Sequential execution (default)
val sequential = for {
  user1 <- userTable.persist(Map("name" -> "Alice"))  // Executes first
  user2 <- userTable.persist(Map("name" -> "Bob"))    // Executes after user1
  user3 <- userTable.persist(Map("name" -> "Charlie")) // Executes after user2
} yield (user1, user2, user3)
```

For **independent** operations (no dependencies), use `FutureTx.sequence` to execute them in a single transaction:

```scala
val users = List("Alice", "Bob", "Charlie", "Dave", "Eve")

// Create independent insert operations
val insertOps: List[FutureTx[Row]] = users.map { name =>
  userTable.persist(Map("name" -> name))
}

// Combine into single transaction
val batchInsert: FutureTx[Seq[Row]] = FutureTx.sequence(insertOps)

// Execute all inserts in one transaction
batchInsert.runSync match {
  case Success(rows) =>
    println(s"Inserted ${rows.length} users in one transaction")

  case Failure(e) =>
    println(s"Batch insert failed - no users created")
}
```

**Key difference:**
- For-comprehension: Sequential, each step depends on previous results
- `FutureTx.sequence`: All operations in one transaction, but no dependencies between them

### Conditional Logic in Transactions

You can use conditional logic within transactions:

```scala
def createUserWithOptionalProfile(
  name: String,
  email: String,
  includeProfile: Boolean
): FutureTx[Row] = for {

  // Always create user
  user <- userTable.persist(Map(
    "name" -> name,
    "email" -> email
  ))

  // Conditionally create profile
  _ <- if (includeProfile) {
    profileTable.persist(Map(
      "user_id" -> user.id,
      "bio" -> "Default bio"
    ))
  } else {
    FutureTx.unit(())  // No-op operation
  }

} yield user

// Usage
createUserWithOptionalProfile("Alice", "alice@example.com", includeProfile = true).runSync
createUserWithOptionalProfile("Bob", "bob@example.com", includeProfile = false).runSync
```

### Nested Transactions (Composition)

You can compose FutureTx values by calling functions that return FutureTx:

```scala
// Helper function that returns FutureTx
def createPost(userId: Long, title: String): FutureTx[Row] = {
  postTable.persist(Map(
    "user_id" -> userId,
    "title" -> title,
    "content" -> s"Content for: $title"
  ))
}

// Compose using the helper
val transaction = for {
  user <- userTable.persist(Map("name" -> "Alice"))

  // Call helper function (returns FutureTx[Row])
  post1 <- createPost(user.id, "First Post")
  post2 <- createPost(user.id, "Second Post")

  // Update count
  _ <- userTable.update(
    Map("post_count" -> 2),
    where(ps"id = ${user.id}")
  )

} yield (user, post1, post2)

// All operations (including those in createPost) execute in ONE transaction
transaction.runSync
```

**Important:** All FutureTx values composed together execute in a **single transaction**, regardless of how they're structured or where they're defined.

### Transaction Composition Pattern

Organize complex transactions using helper functions:

```scala
object UserService {

  // Atomic operation: create user with default settings
  def createUser(name: String, email: String): FutureTx[Row] = for {
    user <- userTable.persist(Map(
      "name" -> name,
      "email" -> email,
      "active" -> true,
      "created_at" -> System.currentTimeMillis()
    ))

    _ <- settingsTable.persist(Map(
      "user_id" -> user.id,
      "notifications" -> true,
      "theme" -> "light"
    ))

  } yield user

  // Atomic operation: create post and update count
  def createPost(userId: Long, title: String, content: String): FutureTx[Row] = for {
    post <- postTable.persist(Map(
      "user_id" -> userId,
      "title" -> title,
      "content" -> content,
      "published" -> false
    ))

    // Increment user's post count
    _ <- userTable.update(
      Map("post_count" -> ps"post_count + 1"),
      where(ps"id = $userId")
    )

  } yield post

  // Compose atomic operations into larger transaction
  def onboardNewUser(name: String, email: String): FutureTx[(Row, Row)] = for {
    user <- createUser(name, email)
    welcomePost <- createPost(user.id, "Welcome!", "Thanks for joining!")
  } yield (user, welcomePost)
}

// Usage - all operations in ONE transaction
UserService.onboardNewUser("Alice", "alice@example.com").runSync
```

---

## Error Handling

sss.db provides a structured exception hierarchy for proper error handling.

### Exception Hierarchy

```
Throwable
  ├─ Error
  │   └─ DbError (unrecoverable: config, schema issues)
  │
  └─ Exception
      └─ RuntimeException
          └─ DbException (recoverable: constraints, deadlocks, timeouts)
              └─ DbOptimisticLockingException (version conflict)
```

**DbOptimisticLockingException:**
- Thrown when optimistic locking version conflict occurs
- Another transaction modified the row between read and write
- **Recoverable** - retry with fresh data

**DbException:**
- Base class for recoverable database errors
- Includes: constraint violations, deadlocks, timeouts, network issues
- **Recoverable** - log, retry, or handle gracefully

**DbError:**
- Unrecoverable errors: configuration problems, schema mismatches
- **Not recoverable** - fix configuration or schema and restart

### Synchronous Error Handling

With `SyncRunContext`, use pattern matching on `Try[T]`:

```scala
import scala.util.{Success, Failure}

implicit val sync = new SyncRunContext(global)

val transaction = for {
  user <- userTable.persist(Map("name" -> "Alice", "email" -> "alice@example.com"))
  post <- postTable.persist(Map("user_id" -> user.id, "title" -> "Hello"))
} yield (user, post)

transaction.runSync match {
  case Success((user, post)) =>
    println(s"✓ Success: created user ${user.id} and post ${post.id}")
    // Continue with business logic

  case Failure(e: DbOptimisticLockingException) =>
    println("⚠ Optimistic locking conflict - retry with fresh version")
    // Implement retry logic (see retry patterns below)

  case Failure(e: DbException) =>
    logger.error(s"Database error: ${e.getMessage}", e)
    // Log and possibly retry
    // Return error response to user

  case Failure(e: DbError) =>
    logger.error(s"Fatal database error: ${e.getMessage}", e)
    // Don't retry - needs configuration/schema fix
    throw e  // Propagate to top-level error handler

  case Failure(e) =>
    logger.error(s"Unexpected error: ${e.getMessage}", e)
    throw e  // Propagate
}
```

### Asynchronous Error Handling

With `AsyncRunContext`, use `Future` combinators:

```scala
import scala.concurrent.Future

implicit val async = new AsyncRunContext(global)

val transaction = for {
  user <- userTable.persist(Map("name" -> "Alice"))
  post <- postTable.persist(Map("user_id" -> user.id))
} yield (user, post)

val futureResult: Future[(Row, Row)] = transaction.run

futureResult.map { case (user, post) =>
  println(s"✓ Success: created user ${user.id} and post ${post.id}")
  (user, post)
}.recover {
  case e: DbOptimisticLockingException =>
    logger.warn("Optimistic locking conflict - retry")
    // Return retry indicator or default value

  case e: DbException =>
    logger.error(s"Database error: ${e.getMessage}", e)
    // Return error response

  case e: DbError =>
    logger.error(s"Fatal error: ${e.getMessage}", e)
    throw e

}.recoverWith {
  // For cases that need async retry
  case e: DbOptimisticLockingException =>
    retryAsync(transaction, maxRetries = 3)
}
```

### Retry Patterns for Optimistic Locking

When optimistic locking fails, retry with fresh data:

**Simple retry with exponential backoff (synchronous):**

```scala
def persistWithRetry[T](
  op: FutureTx[T],
  maxRetries: Int = 3
)(implicit sync: SyncRunContext): Try[T] = {

  (1 to maxRetries).foreach { attempt =>
    op.runSync match {
      case s @ Success(_) =>
        return s

      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        val backoffMs = 100 * attempt  // 100ms, 200ms, 300ms
        logger.warn(s"Optimistic locking conflict - retry $attempt/$maxRetries after ${backoffMs}ms")
        Thread.sleep(backoffMs)

      case f @ Failure(_) =>
        return f
    }
  }

  Failure(new Exception(s"Max retries ($maxRetries) exceeded"))
}

// Usage
val operation = userTable.persist(Map("id" -> 123, "name" -> "Updated", "version" -> currentVersion))
persistWithRetry(operation, maxRetries = 5).get
```

**Retry with fresh data fetch (synchronous):**

```scala
def updateWithRetry(
  userId: Long,
  updateFn: Row => Map[String, Any],
  maxRetries: Int = 3
)(implicit sync: SyncRunContext): Try[Row] = {

  (1 to maxRetries).foreach { attempt =>
    // Fetch fresh data
    val currentRow = userTable(userId).runSyncAndGet

    // Apply update function
    val updates = updateFn(currentRow)

    // Attempt persist (includes current version)
    userTable.persist(currentRow.asMap ++ updates).runSync match {
      case s @ Success(_) =>
        return s

      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        logger.warn(s"Optimistic locking conflict - retry $attempt/$maxRetries")
        Thread.sleep(100 * attempt)

      case f @ Failure(_) =>
        return f
    }
  }

  Failure(new Exception(s"Max retries ($maxRetries) exceeded"))
}

// Usage
updateWithRetry(userId = 123, maxRetries = 5) { currentRow =>
  Map("balance" -> (currentRow.double("balance") + 100.0))
}
```

**Async retry with Future:**

```scala
def retryAsync[T](
  op: FutureTx[T],
  maxRetries: Int,
  attempt: Int = 1
)(implicit async: AsyncRunContext): Future[T] = {

  op.run.recoverWith {
    case e: DbOptimisticLockingException if attempt < maxRetries =>
      val backoffMs = 100 * attempt
      logger.warn(s"Retry $attempt/$maxRetries after ${backoffMs}ms")

      // Delay and retry
      akka.pattern.after(backoffMs.milliseconds, system.scheduler) {
        retryAsync(op, maxRetries, attempt + 1)
      }

    case e =>
      Future.failed(e)  // Give up
  }
}

// Usage
retryAsync(
  userTable.persist(Map("id" -> 123, "name" -> "Updated")),
  maxRetries = 5
)
```

### Circuit Breaker Pattern

For resilient systems, combine retries with circuit breakers:

```scala
import akka.pattern.CircuitBreaker
import scala.concurrent.duration._

val breaker = new CircuitBreaker(
  system.scheduler,
  maxFailures = 5,
  callTimeout = 10.seconds,
  resetTimeout = 1.minute
)

def resilientPersist[T](op: FutureTx[T])(implicit async: AsyncRunContext): Future[T] = {
  breaker.withCircuitBreaker(
    retryAsync(op, maxRetries = 3)
  )
}
```

---

## Transaction Isolation Levels

Transaction isolation controls what changes are visible between concurrent transactions.

### Available Isolation Levels

sss.db supports all standard SQL isolation levels:

```scala
import sss.db.TxIsolationLevel._

// READ_UNCOMMITTED: Lowest isolation, highest performance (dirty reads possible)
// READ_COMMITTED: Default - no dirty reads (most common)
// REPEATABLE_READ: No dirty or non-repeatable reads
// SERIALIZABLE: Highest isolation, lowest performance (no anomalies)
```

### Configuring Isolation Level

Set isolation level when creating RunContext:

```scala
import sss.db.TxIsolationLevel._
import scala.concurrent.ExecutionContext.Implicits.global

// Synchronous with SERIALIZABLE isolation
implicit val sync = new SyncRunContext(
  ds = dataSource,
  timeout = 5.seconds,
  isolationLevel = Some(SERIALIZABLE)
)

// Asynchronous with REPEATABLE_READ isolation
implicit val async = new AsyncRunContext(
  aDs = dataSource,
  anEc = global,
  isolationLevel = Some(REPEATABLE_READ)
)
```

### Isolation Level Comparison

| Level | Dirty Read | Non-Repeatable Read | Phantom Read | Performance |
|-------|-----------|---------------------|--------------|-------------|
| READ_UNCOMMITTED | ✓ Possible | ✓ Possible | ✓ Possible | ⚡ Fastest |
| READ_COMMITTED | ✗ Prevented | ✓ Possible | ✓ Possible | ⚡⚡ Fast |
| REPEATABLE_READ | ✗ Prevented | ✗ Prevented | ✓ Possible | ⚡⚡⚡ Moderate |
| SERIALIZABLE | ✗ Prevented | ✗ Prevented | ✗ Prevented | ⚡⚡⚡⚡ Slowest |

**Definitions:**
- **Dirty Read**: Reading uncommitted changes from another transaction
- **Non-Repeatable Read**: Same query returns different results if another transaction commits between reads
- **Phantom Read**: Same query returns different rows if another transaction inserts/deletes between reads

### When to Use Each Level

**READ_COMMITTED (Default):**
```scala
// Most common choice - good balance of safety and performance
// Use for: Web APIs, standard CRUD operations
implicit val sync = new SyncRunContext(global, isolationLevel = Some(READ_COMMITTED))
```

**REPEATABLE_READ:**
```scala
// Use when you need consistent reads within a transaction
// Example: Generating reports that must be internally consistent
implicit val sync = new SyncRunContext(global, isolationLevel = Some(REPEATABLE_READ))

val report = for {
  totalUsers <- userTable.count().map(_.toLong)
  totalPosts <- postTable.count().map(_.toLong)
  avgPostsPerUser = if (totalUsers > 0) totalPosts.toDouble / totalUsers else 0.0
} yield Report(totalUsers, totalPosts, avgPostsPerUser)
// totalUsers and totalPosts are consistent snapshots
```

**SERIALIZABLE:**
```scala
// Use for: Financial transactions, inventory management, critical updates
// Prevents all anomalies but can cause deadlocks
implicit val sync = new SyncRunContext(global, isolationLevel = Some(SERIALIZABLE))

val transfer = for {
  fromAccount <- accountTable(fromId)
  toAccount <- accountTable(toId)

  _ <- accountTable.update(
    Map("balance" -> (fromAccount.double("balance") - amount)),
    where(ps"id = $fromId")
  )

  _ <- accountTable.update(
    Map("balance" -> (toAccount.double("balance") + amount)),
    where(ps"id = $toId")
  )
} yield ()
// No concurrent transfers can interleave
```

**READ_UNCOMMITTED:**
```scala
// Rarely used - only for non-critical reads where performance is critical
// Example: Approximate analytics on large datasets
implicit val sync = new SyncRunContext(global, isolationLevel = Some(READ_UNCOMMITTED))
```

---

## Optimistic Locking

Optimistic locking prevents lost updates when multiple transactions modify the same row.

### How It Works

1. Table has a `version` column (BIGINT)
2. On read: Fetch current version
3. On update: Increment version and check old version matches
4. If version changed: Another transaction modified the row → throw `DbOptimisticLockingException`

### Enabling Optimistic Locking

Add a `version` column to your table:

```sql
CREATE TABLE accounts (
  id BIGINT PRIMARY KEY,
  balance DECIMAL(10, 2),
  version BIGINT  -- sss.db automatically detects this
)
```

**That's it!** sss.db automatically enables optimistic locking when it detects a `version` column.

### Basic Usage

```scala
// Read account (includes version)
val account = accountTable(accountId).runSyncAndGet

// Update account
val updated = accountTable.persist(Map(
  "id" -> account.id,
  "balance" -> account.double("balance") + 100.0,
  "version" -> account.long("version")  // Include current version
)).runSync

updated match {
  case Success(row) =>
    println(s"✓ Balance updated, new version: ${row.long("version")}")

  case Failure(e: DbOptimisticLockingException) =>
    println("⚠ Another transaction modified this account - retry")

  case Failure(e) =>
    println(s"✗ Update failed: ${e.getMessage}")
}
```

### Automatic Version Management

sss.db automatically:
- ✅ Checks version matches on update
- ✅ Increments version on successful update
- ✅ Throws `DbOptimisticLockingException` if version changed

You don't need to manually increment version or write version-checking SQL.

### Retry Pattern with Optimistic Locking

```scala
def transferFunds(
  fromAccountId: Long,
  toAccountId: Long,
  amount: Double,
  maxRetries: Int = 3
)(implicit sync: SyncRunContext): Try[Unit] = {

  (1 to maxRetries).foreach { attempt =>
    val transfer = for {
      // Fetch current state (includes versions)
      fromAccount <- accountTable(fromAccountId)
      toAccount <- accountTable(toAccountId)

      // Validate
      _ = require(fromAccount.double("balance") >= amount, "Insufficient funds")

      // Update from account
      _ <- accountTable.persist(Map(
        "id" -> fromAccountId,
        "balance" -> (fromAccount.double("balance") - amount),
        "version" -> fromAccount.long("version")
      ))

      // Update to account
      _ <- accountTable.persist(Map(
        "id" -> toAccountId,
        "balance" -> (toAccount.double("balance") + amount),
        "version" -> toAccount.long("version")
      ))

    } yield ()

    transfer.runSync match {
      case s @ Success(_) =>
        return s

      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        logger.warn(s"Optimistic locking conflict - retry $attempt/$maxRetries")
        Thread.sleep(100 * attempt)  // Exponential backoff

      case f @ Failure(_) =>
        return f
    }
  }

  Failure(new Exception(s"Transfer failed after $maxRetries retries"))
}
```

### Optimistic vs Pessimistic Locking

**Optimistic Locking (sss.db default):**
- ✅ Better performance (no locks held)
- ✅ No deadlocks
- ❌ Requires retry logic
- Best for: Low contention scenarios

**Pessimistic Locking (SELECT FOR UPDATE):**
- ✅ No retry needed
- ❌ Holds locks (reduces concurrency)
- ❌ Can cause deadlocks
- Best for: High contention scenarios

sss.db uses optimistic locking because it's simpler and performs better in most cases. For high-contention scenarios, consider pessimistic locking at the database level.

---

## Connection Management

### Connection Pool Configuration

sss.db uses HikariCP for connection pooling. Configure in `application.conf`:

```hocon
database {
  datasource {
    driver = "org.postgresql.Driver"
    connection = "jdbc:postgresql://localhost:5432/mydb"
    user = "myuser"
    pass = "mypass"

    # Pool size
    maxPoolSize = 20                    # Max concurrent connections
    minimumIdle = 5                     # Min idle connections maintained

    # Timeouts (milliseconds)
    connectionTimeout = 30000           # Wait 30s for connection
    idleTimeout = 600000                # Idle connection lifetime (10m)
    maxLifetime = 1800000               # Max connection lifetime (30m)

    # Performance
    cachePrepStmts = true               # Cache prepared statements
    prepStmtCacheSize = 250
    prepStmtCacheSqlLimit = 2048
  }
}
```

### Connection Pool Sizing

**Formula:**
```
maxPoolSize = (max_concurrent_transactions + buffer)
```

**Guidelines:**

**For SyncRunContext:**
- Pool size ≈ number of concurrent blocking threads
- Example: If 20 concurrent requests, use `maxPoolSize = 25` (20 + buffer)

**For AsyncRunContext:**
- Smaller pool OK (futures queue efficiently)
- Pool size ≈ number of CPU cores × 2
- Example: 8-core machine → `maxPoolSize = 16`

**Monitoring:**
```scala
// HikariCP exposes JMX metrics
// Monitor:
// - Active connections
// - Idle connections
// - Connection wait time
// - Connection acquisition time
```

### Timeout Configuration

**Default timeout (SyncRunContext):**
```scala
// Default: 3 seconds
implicit val sync = new SyncRunContext(global)  // timeout = 3.seconds
```

**Custom timeout for long operations:**
```scala
import scala.concurrent.duration._

// 30-second timeout for batch jobs
implicit val longRunningSync = new SyncRunContext(global, timeout = 30.seconds)
```

### Resource Cleanup Guarantees

sss.db **guarantees** resource cleanup even on exception:

```scala
// Internally, sss.db uses try/finally:
def executeTransaction[T](op: FutureTx[T]): Try[T] = {
  val conn = pool.getConnection()
  try {
    conn.setAutoCommit(false)
    val result = op.apply(TransactionContext(conn, ec))
    conn.commit()
    Success(result)
  } catch {
    case e: Exception =>
      conn.rollback()
      Failure(e)
  } finally {
    // ALWAYS runs, even if exception thrown
    conn.close()  // Returns connection to pool
  }
}
```

**What's guaranteed:**
- ✅ Connections always returned to pool
- ✅ PreparedStatements always closed
- ✅ ResultSets always closed
- ✅ Rollback on exception
- ✅ Blobs freed (if `freeBlobsEarly = true`)

---

## Best Practices

### 1. Keep Transactions Short

```scala
// ❌ Bad: Long-running transaction holds connection
val longTransaction = for {
  users <- userTable.findAll()
  _ = processUsers(users)  // Long computation!
  _ = sendEmails(users)    // Network I/O!
  results <- userTable.persist(...)
} yield results

// ✅ Good: Fetch data, release connection, then process
val users = userTable.findAll().runSyncAndGet
processUsers(users)  // Outside transaction
sendEmails(users)    // Outside transaction

// New transaction for updates
userTable.persist(...).runSync
```

### 2. Use Batch Operations

```scala
// ❌ Bad: N separate transactions
users.foreach { user =>
  userTable.persist(user).runSync
}

// ✅ Good: One transaction
val inserts = users.map(userTable.persist)
FutureTx.sequence(inserts).runSync
```

### 3. Handle Optimistic Locking

```scala
// ❌ Bad: No retry logic
accountTable.persist(updatedData).runSync

// ✅ Good: Retry on version conflict
persistWithRetry(
  accountTable.persist(updatedData),
  maxRetries = 3
)
```

### 4. Use Appropriate Isolation Level

```scala
// ❌ Bad: SERIALIZABLE for everything (slow!)
implicit val sync = new SyncRunContext(global, isolationLevel = Some(SERIALIZABLE))

// ✅ Good: Use READ_COMMITTED for most cases
implicit val sync = new SyncRunContext(global, isolationLevel = Some(READ_COMMITTED))

// ✅ Good: Use SERIALIZABLE only when needed
def criticalTransaction()(implicit sync: SyncRunContext) = {
  // Temporarily use SERIALIZABLE for this transaction only
  implicit val serializable = new SyncRunContext(
    sync.ds,
    sync.timeout,
    isolationLevel = Some(SERIALIZABLE)
  )
  // ... critical operation
}
```

### 5. Don't Swallow Exceptions

```scala
// ❌ Bad: Swallowing exceptions
try {
  userTable.persist(data).runSyncAndGet
} catch {
  case e: Exception => // Silent failure!
}

// ✅ Good: Handle or propagate
userTable.persist(data).runSync match {
  case Success(row) => row
  case Failure(e: DbException) =>
    logger.error("Database error", e)
    throw e  // Or return error to caller
  case Failure(e) => throw e
}
```

### 6. Monitor Connection Pool

```scala
// ❌ Bad: No monitoring
db.shutdown.runSyncAndGet

// ✅ Good: Monitor metrics
// - Track connection acquisition time
// - Alert on pool exhaustion
// - Monitor active vs idle connections
// - Track transaction duration
```

---

## Summary

### Key Takeaways

**Transactions:**
- Provide ACID guarantees (Atomicity, Consistency, Isolation, Durability)
- Lazy execution (only run when you call `.run` or `.runSync`)
- Automatic commit/rollback based on success/failure

**Composing:**
- Use for-comprehensions for sequential operations
- Use `FutureTx.sequence` for independent operations
- All composed operations execute in a single transaction

**Error Handling:**
- `DbOptimisticLockingException`: Version conflict, retry with fresh data
- `DbException`: Recoverable errors, log and possibly retry
- `DbError`: Unrecoverable, fix config/schema

**Isolation Levels:**
- `READ_COMMITTED`: Default, good for most cases
- `REPEATABLE_READ`: Consistent reads within transaction
- `SERIALIZABLE`: Highest safety, lowest performance

**Optimistic Locking:**
- Automatic when table has `version` column
- Prevents lost updates
- Requires retry logic

**Best Practices:**
- Keep transactions short
- Use batch operations
- Handle optimistic locking with retries
- Use appropriate isolation level
- Monitor connection pool

### Next Steps

- **[Queries](queries.md)** - Master the query DSL
- **[CRUD Operations](crud-operations.md)** - Comprehensive CRUD patterns
- **[Performance](performance.md)** - Optimize database access
- **[Troubleshooting](troubleshooting.md)** - Common transaction issues
