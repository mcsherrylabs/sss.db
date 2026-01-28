# Troubleshooting

This guide helps you diagnose and fix common production issues with sss.db. Each section is organized by symptom for quick problem resolution.

## Table of Contents

- [Connection Pool Exhausted](#connection-pool-exhausted)
- [Slow Queries](#slow-queries)
- [Optimistic Locking Failures](#optimistic-locking-failures)
- [Memory Leaks](#memory-leaks)
- [Transaction Deadlocks](#transaction-deadlocks)
- [Configuration Errors](#configuration-errors)

---

## Connection Pool Exhausted

### Symptom

```
java.sql.SQLTransientConnectionException: HikariPool - Connection is not available
Timeout after 30000ms of waiting for connection
```

### Common Causes

1. Pool too small for workload
2. Connection leaks (not returned to pool)
3. Long-running transactions holding connections
4. Sudden traffic spike

### Diagnostics

1. **Check active connections:**
```scala
// Enable JMX monitoring
datasource {
  registerMbeans = true
}
// Monitor: HikariCP -> Pool -> ActiveConnections
```

2. **Measure transaction duration:**
```scala
val start = System.currentTimeMillis()
val result = transaction.runSync
val duration = System.currentTimeMillis() - start
println(s"Transaction took ${duration}ms")
```

3. **Check logs for slow queries**

### Solutions

**Solution 1: Increase pool size**
```hocon
datasource {
  maxPoolSize = 30  # Increase from default
}
```

**Solution 2: Shorten transactions**
```scala
// ❌ Bad: Long transaction
val transaction = for {
  data <- table.findAll()
  _ = slowProcessing(data)  // Don't do this!
  result <- table.persist(...)
} yield result

// ✅ Good: Short transaction
val data = table.findAll().runSyncAndGet
slowProcessing(data)  // Outside transaction
table.persist(...).runSync
```

**Solution 3: Add connection timeout**
```hocon
datasource {
  connectionTimeout = 30000  # 30 seconds
}
```

**Solution 4: Monitor and alert**
- Alert if wait time > 100ms
- Alert if active connections > 80% of maxPoolSize

---

## Slow Queries

### Symptom

Operations taking >1 second to complete.

### Common Causes

1. Missing database indexes
2. N+1 query problem
3. Large result sets loaded into memory
4. Full table scans

### Diagnostics

1. **Enable query logging:**
```hocon
# PostgreSQL
datasource {
  connectionProperties = "log_statement=all;log_duration=on"
}
```

2. **Check execution plan:**
```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE email = 'alice@example.com';
```

3. **Measure query duration:**
```scala
val start = System.currentTimeMillis()
val users = userTable.filter(where(ps"active = ${true}")).runSyncAndGet
println(s"Query took ${System.currentTimeMillis() - start}ms")
```

### Solutions

**Solution 1: Add indexes**
```sql
-- Index frequently queried columns
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_created_at ON posts(created_at DESC);
```

**Solution 2: Fix N+1 queries**
```scala
// ❌ N+1 problem
val users = userTable.findAll().runSyncAndGet
users.foreach { user =>
  val posts = postTable.filter(where(ps"user_id = ${user.id}")).runSyncAndGet
}

// ✅ Batch query
val users = userTable.findAll().runSyncAndGet
val userIds = users.map(_.id)
val posts = postTable.filter(where("user_id").in(userIds)).runSyncAndGet
```

**Solution 3: Use pagination**
```scala
// ❌ Load everything
val allUsers = userTable.findAll().runSyncAndGet  // Slow!

// ✅ Use PagedView
val pagedView = userTable.toPaged(pageSize = 1000)
pagedView.toIterator.foreach(processUser)
```

**Solution 4: Fetch only needed columns**
```scala
// ✅ Specific columns only
val userView = new View(
  tableName = "users",
  where = Where(),
  runContext = db.syncRunContext,
  freeBlobsEarly = false,
  cols = "id, name, email"  # Don't fetch large TEXT/BLOB columns
)
```

---

## Optimistic Locking Failures

### Symptom

```
DbOptimisticLockingException: Version mismatch - row was modified by another transaction
```

### Common Causes

1. High contention (many concurrent updates to same row)
2. Long-running transactions
3. No retry logic implemented

### Diagnostics

1. **Check version conflicts frequency:**
```scala
var conflicts = 0
(1 to 100).foreach { _ =>
  accountTable.persist(accountData).runSync match {
    case Failure(_: DbOptimisticLockingException) => conflicts += 1
    case _ =>
  }
}
println(s"Conflict rate: ${conflicts}%")
```

2. **Monitor transaction duration**
3. **Check concurrent update patterns**

### Solutions

**Solution 1: Implement retry with backoff**
```scala
def persistWithRetry[T](
  op: FutureTx[T],
  maxRetries: Int = 3
)(implicit sync: SyncRunContext): Try[T] = {
  (1 to maxRetries).foreach { attempt =>
    op.runSync match {
      case s @ Success(_) => return s
      case Failure(e: DbOptimisticLockingException) if attempt < maxRetries =>
        Thread.sleep(100 * attempt)  // Exponential backoff
      case f @ Failure(_) => return f
    }
  }
  Failure(new Exception("Max retries exceeded"))
}
```

**Solution 2: Fetch fresh data before retry**
```scala
def updateWithFreshData(userId: Long): Try[Row] = {
  (1 to 3).foreach { attempt =>
    // Fetch current state
    val current = userTable(userId).runSyncAndGet

    // Apply update
    val updated = current.asMap ++ Map("balance" -> newBalance)

    userTable.persist(updated).runSync match {
      case s @ Success(_) => return s
      case Failure(_: DbOptimisticLockingException) if attempt < 3 =>
        Thread.sleep(100 * attempt)
      case f @ Failure(_) => return f
    }
  }
  Failure(new Exception("Max retries exceeded"))
}
```

**Solution 3: Reduce transaction duration**
```scala
// ❌ Long transaction increases conflict chance
val transaction = for {
  account <- accountTable(id)
  _ = complexCalculation()  // Long!
  updated <- accountTable.persist(account.asMap ++ updates)
} yield updated

// ✅ Short transaction
val account = accountTable(id).runSyncAndGet
val newData = complexCalculation(account)  // Outside transaction
accountTable.persist(newData).runSync  // Quick update
```

**Solution 4: Consider pessimistic locking**

For very high contention, use database-level locks:
```sql
SELECT * FROM accounts WHERE id = ? FOR UPDATE;
```

---

## Memory Leaks

### Symptom

```
java.lang.OutOfMemoryError: Java heap space
```

Gradual memory growth over time.

### Common Causes

1. Blobs not extracted inside transactions
2. Large result sets not released
3. Connection leaks
4. Cached View instances not garbage collected

### Diagnostics

1. **Heap dump analysis:**
```bash
jmap -dump:live,format=b,file=heap.bin <pid>
jvisualvm heap.bin
```

2. **Check for blob usage:**
```bash
grep -r "blobByteArray\|blobInputStream" src/
```

3. **Monitor memory usage:**
```scala
val runtime = Runtime.getRuntime
println(s"Used memory: ${(runtime.totalMemory - runtime.freeMemory) / 1024 / 1024}MB")
```

### Solutions

**Solution 1: Extract blobs inside transactions**
```scala
// ❌ Wrong - blob extracted outside transaction
val row = blobTable(id).runSyncAndGet
val data = row.blobByteArray("content")  // May leak!

// ✅ Correct - extracted inside
val data = (for {
  row <- blobTable(id)
  bytes = row.blobByteArray("content")
} yield bytes).runSyncAndGet
```

**Solution 2: Enable freeBlobsEarly**
```hocon
database {
  freeBlobsEarly = true  # Release blob memory immediately
}
```

**Solution 3: Use pagination for large result sets**
```scala
// ❌ Loads everything into memory
val allUsers = userTable.findAll().runSyncAndGet

// ✅ Stream with PagedView
userTable.toPaged(1000).toIterator.foreach(processUser)
```

**Solution 4: Ensure connections are closed**
```scala
// Always shutdown database
override def afterAll(): Unit = {
  db.shutdown.runSyncAndGet
}
```

---

## Transaction Deadlocks

### Symptom

```
java.sql.SQLException: Deadlock detected
Transaction rolled back due to deadlock
```

### Common Causes

1. Inconsistent lock ordering
2. Long-running transactions
3. High contention
4. Serializable isolation level

### Diagnostics

1. **Check database deadlock logs:**
```sql
-- PostgreSQL
SELECT * FROM pg_stat_activity WHERE wait_event_type = 'Lock';

-- MySQL
SHOW ENGINE INNODB STATUS;
```

2. **Identify lock patterns:**
```scala
// Log transaction operations
println(s"Acquiring lock on user ${userId}")
userTable(userId).runSyncAndGet
println(s"Acquiring lock on account ${accountId}")
accountTable(accountId).runSyncAndGet
```

### Solutions

**Solution 1: Consistent lock ordering**
```scala
// ❌ Inconsistent ordering causes deadlocks
// Transaction 1:
for {
  user <- userTable(userId1)
  account <- accountTable(accountId1)
} yield ()

// Transaction 2:
for {
  account <- accountTable(accountId1)  // Different order!
  user <- userTable(userId1)
} yield ()

// ✅ Consistent ordering
// Always acquire locks in same order
for {
  user <- userTable(userId)      // Always user first
  account <- accountTable(accountId)  // Then account
} yield ()
```

**Solution 2: Shorten transactions**
```scala
// ❌ Long transaction increases deadlock chance
val transaction = for {
  user <- userTable(userId)
  _ = slowProcessing()  // Long!
  account <- accountTable(accountId)
} yield ()

// ✅ Short transaction
val user = userTable(userId).runSyncAndGet
slowProcessing()
accountTable(accountId).runSyncAndGet
```

**Solution 3: Retry on deadlock**
```scala
def withDeadlockRetry[T](op: => T, maxRetries: Int = 3): T = {
  (1 to maxRetries).foreach { attempt =>
    try {
      return op
    } catch {
      case e: SQLException if e.getMessage.contains("deadlock") =>
        if (attempt == maxRetries) throw e
        Thread.sleep(50 * attempt)  // Backoff
    }
  }
  throw new Exception("Max retries exceeded")
}
```

**Solution 4: Reduce isolation level**
```hocon
datasource {
  # Use READ_COMMITTED instead of SERIALIZABLE
  transactionIsolationLevel = "TRANSACTION_READ_COMMITTED"
}
```

---

## Configuration Errors

### Symptom

```
com.zaxxer.hikari.pool.HikariPool: Exception during pool initialization
Unable to load class: org.postgresql.Driver
```

### Common Causes

1. Missing JDBC driver dependency
2. Invalid connection string
3. Wrong credentials
4. Missing configuration file

### Diagnostics

1. **Check classpath:**
```bash
sbt "show fullClasspath"
```

2. **Verify configuration loaded:**
```scala
val config = ConfigFactory.load()
println(config.getString("database.datasource.driver"))
```

3. **Test connection:**
```scala
try {
  val db = Db("database")
  println("✓ Connection successful")
  db.shutdown.runSyncAndGet
} catch {
  case e: Exception =>
    println(s"✗ Connection failed: ${e.getMessage}")
}
```

### Solutions

**Solution 1: Add JDBC driver**
```scala
// build.sbt
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.0"
```

**Solution 2: Validate connection string**
```hocon
datasource {
  # Correct format for PostgreSQL
  connection = "jdbc:postgresql://host:port/database"

  # Common mistakes:
  # ❌ "postgresql://host:port/database"  (missing jdbc:)
  # ❌ "jdbc:postgres://..."               (wrong driver name)
}
```

**Solution 3: Check credentials**
```bash
# Test database connection directly
psql -h localhost -U myuser -d mydb

# If that fails, check:
# - User exists
# - User has permissions
# - pg_hba.conf allows connections
```

**Solution 4: Verify config file location**
```bash
# Should be in:
src/main/resources/application.conf  # For main code
src/test/resources/application.conf  # For tests
```

---

## Quick Diagnostic Checklist

When facing issues, check:

- [ ] Database is running and accessible
- [ ] JDBC driver is in classpath
- [ ] Connection string is correct
- [ ] Credentials are valid
- [ ] Connection pool has capacity
- [ ] Transactions are short
- [ ] Indexes exist on queried columns
- [ ] No N+1 query patterns
- [ ] Blobs extracted inside transactions
- [ ] Configuration file is loaded

---

## Getting More Help

**Enable detailed logging:**
```hocon
# logback.xml
<logger name="com.zaxxer.hikari" level="DEBUG"/>
<logger name="sss.db" level="DEBUG"/>
```

**Resources:**
- [Configuration Guide](configuration.md) - Connection pool tuning
- [Performance Guide](performance.md) - Optimization patterns
- [Transactions Guide](transactions.md) - Transaction troubleshooting
- [GitHub Issues](https://github.com/mcsherrylabs/sss.db/issues) - Report bugs
