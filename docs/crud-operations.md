# CRUD Operations

This guide provides a comprehensive reference for Create, Read, Update, and Delete operations in sss.db. You'll learn all the patterns for manipulating data, including batch operations and special handling for blobs.

**Prerequisites:** Read [Getting Started](getting-started.md) and [Queries](queries.md) first.

## Table of Contents

- [Create (Insert)](#create-insert)
- [Read (Query)](#read-query)
- [Update](#update)
- [Delete](#delete)
- [Blob Handling](#blob-handling)
- [Batch Operations](#batch-operations)
- [Best Practices](#best-practices)

---

## Create (Insert)

### insert() - Explicit Insert

Creates a new row with auto-generated ID:

```scala
val db = Db("database")
val userTable = db.table("users")

// Insert new row
val user: Row = userTable.insert(Map(
  "name" -> "Alice",
  "email" -> "alice@example.com",
  "age" -> 30,
  "active" -> true
)).runSyncAndGet

println(s"Created user with id: ${user.id}")
println(s"Name: ${user.string("name")}")
```

**Returns:** `Row` with auto-generated ID populated

**Use when:** You want to explicitly insert a new row

### persist() - Insert or Update (Upsert)

Inserts if the row doesn't exist, updates if it does:

```scala
// First call: Inserts new row
val user = userTable.persist(Map(
  "name" -> "Bob",
  "email" -> "bob@example.com"
)).runSyncAndGet

println(s"Created user: ${user.id}")

// Second call: Updates existing row
val updated = userTable.persist(Map(
  "id" -> user.id,
  "name" -> "Bob Smith",  // Changed name
  "email" -> "bob@example.com"
)).runSyncAndGet

println(s"Updated user: ${updated.id}")
```

**How it works:**
- If `Map` contains primary key and row exists → **UPDATE**
- If `Map` doesn't contain primary key or row doesn't exist → **INSERT**

**Use when:** You don't care whether you're inserting or updating

### insertNoIdentity() - Insert Without Auto-Increment

For tables where you provide the ID:

```scala
// Table without auto-increment primary key
val configTable = db.table("config")

configTable.insertNoIdentity(Map(
  "id" -> "app.version",  // You provide the ID
  "value" -> "1.0.0"
)).runSyncAndGet
```

**Use when:**
- Table has non-auto-increment primary key
- UUID or string-based primary keys
- Manual ID assignment

### Providing All Columns

```scala
// Explicit about all columns
userTable.insert(Map(
  "name" -> "Charlie",
  "email" -> "charlie@example.com",
  "age" -> 25,
  "active" -> true,
  "created_at" -> System.currentTimeMillis(),
  "updated_at" -> System.currentTimeMillis()
)).runSyncAndGet
```

### Nullable Columns

Use `None` for NULL values:

```scala
userTable.insert(Map(
  "name" -> "Dave",
  "email" -> "dave@example.com",
  "age" -> 40,
  "middle_name" -> None,  // NULL
  "nickname" -> Some("Big D"),  // Some(value) or value directly
  "active" -> true
)).runSyncAndGet
```

**Both work:**
- `"column" -> None` → NULL
- `"column" -> Some(value)` → value
- `"column" -> value` → value (if not wrapped in Option)

### Type Conversion

sss.db handles common type conversions automatically:

```scala
import java.util.Date
import java.sql.Timestamp

userTable.insert(Map(
  "name" -> "Eve",
  "age" -> 35,  // Int
  "salary" -> 75000.50,  // Double
  "active" -> true,  // Boolean
  "created_at" -> new Date().getTime,  // Long (timestamp)
  "birth_date" -> new java.sql.Date(System.currentTimeMillis()),  // SQL Date
  "last_login" -> new Timestamp(System.currentTimeMillis()),  // SQL Timestamp
  "metadata" -> """{"key": "value"}"""  // String (JSON)
)).runSyncAndGet
```

---

## Read (Query)

Read operations are covered comprehensively in the [Queries guide](queries.md). Here's a quick reference:

### Basic Reads

```scala
// Find by ID (returns Option[Row])
val user: Option[Row] = userTable.find(where("id" -> 1)).runSyncAndGet

// Get by ID (returns Row or throws)
val user: Row = userTable(1).runSyncAndGet

// Filter (returns Seq[Row])
val activeUsers: Seq[Row] = userTable.filter(
  where(ps"active = ${true}")
).runSyncAndGet

// Find all
val allUsers: Seq[Row] = userTable.findAll().runSyncAndGet
```

### Extracting Values

```scala
val user = userTable(1).runSyncAndGet

// Type-safe accessors
val name: String = user.string("name")
val age: Int = user.int("age")
val email: String = user.string("email")
val active: Boolean = user.boolean("active")
val createdAt: Long = user.long("created_at")

// Nullable columns
val middleName: Option[String] = user.stringOpt("middle_name")
val nickname: Option[String] = user.stringOpt("nickname")
```

**See [Queries guide](queries.md) for:**
- WHERE clauses
- Ordering and pagination
- IN/NOT IN clauses
- Aggregation (count, max)
- Advanced filtering

---

## Update

### update() - Update with WHERE Clause

Updates rows matching a condition:

```scala
// Update single row
val rowsUpdated: Int = userTable.update(
  Map("age" -> 31),
  where(ps"id = ${1}")
).runSyncAndGet

println(s"Updated $rowsUpdated row(s)")

// Update multiple rows
val rowsUpdated: Int = userTable.update(
  Map("active" -> false),
  where(ps"last_login < ${thirtyDaysAgo}")
).runSyncAndGet

println(s"Deactivated $rowsUpdated inactive users")
```

**Returns:** Number of rows updated (Int)

### persist() with ID - Upsert Pattern

Update existing row or insert if doesn't exist:

```scala
// Fetch existing row
val user = userTable(123).runSyncAndGet

// Modify some fields
val updatedUser = userTable.persist(Map(
  "id" -> user.id,  // Include ID for update
  "age" -> user.int("age") + 1,  // Increment age
  "updated_at" -> System.currentTimeMillis()
)).runSyncAndGet

println(s"Updated user: ${updatedUser.string("name")}")
```

**Pattern: Update from existing Row:**

```scala
// Fetch, modify, persist
val user = userTable(userId).runSyncAndGet
val updates = user.asMap ++ Map("age" -> 31, "active" -> false)
userTable.persist(updates).runSyncAndGet
```

### Updating Multiple Columns

```scala
userTable.update(
  Map(
    "name" -> "Robert Smith",
    "email" -> "robert.smith@example.com",
    "age" -> 32,
    "updated_at" -> System.currentTimeMillis()
  ),
  where(ps"id = ${userId}")
).runSyncAndGet
```

### Conditional Updates

```scala
// Update only if condition matches
val rowsUpdated = userTable.update(
  Map("verified" -> true),
  where(ps"email = $email AND verified = ${false}")
).runSyncAndGet

if (rowsUpdated > 0) {
  println("User verified")
} else {
  println("User already verified or not found")
}
```

### Incrementing Values

```scala
// Increment a counter
userTable.update(
  Map("login_count" -> ps"login_count + 1"),
  where(ps"id = ${userId}")
).runSyncAndGet

// Decrement balance
accountTable.update(
  Map("balance" -> ps"balance - ${amount}"),
  where(ps"id = ${accountId}")
).runSyncAndGet
```

**Note:** Use `ps""` interpolator for SQL expressions

### Optimistic Locking Behavior

If your table has a `version` column, updates automatically use optimistic locking:

```scala
// Table with version column
CREATE TABLE accounts (
  id BIGINT PRIMARY KEY,
  balance DECIMAL(10, 2),
  version BIGINT  -- Enables optimistic locking
)

// Fetch current state (includes version)
val account = accountTable(accountId).runSyncAndGet

// Update with version check
accountTable.persist(Map(
  "id" -> account.id,
  "balance" -> account.double("balance") + 100.0,
  "version" -> account.long("version")  // Version checked automatically
)).runSync match {
  case Success(updated) =>
    println(s"Balance updated, new version: ${updated.long("version")}")

  case Failure(e: DbOptimisticLockingException) =>
    println("Another transaction modified this account - retry with fresh data")

  case Failure(e) =>
    println(s"Update failed: ${e.getMessage}")
}
```

**See [Transactions guide](transactions.md#optimistic-locking) for retry patterns.**

---

## Delete

### delete() - Delete with WHERE Clause

Deletes rows matching a condition:

```scala
// Delete single row
val rowsDeleted: Int = userTable.delete(
  where(ps"id = ${123}")
).runSyncAndGet

println(s"Deleted $rowsDeleted row(s)")

// Delete multiple rows
val rowsDeleted: Int = userTable.delete(
  where(ps"active = ${false} AND last_login < ${oneYearAgo}")
).runSyncAndGet

println(s"Deleted $rowsDeleted inactive users")
```

**Returns:** Number of rows deleted (Int)

**⚠️ Warning:** Always use a WHERE clause! Omitting WHERE deletes ALL rows:

```scala
// ❌ DANGEROUS - Deletes ALL rows
userTable.delete(where("")).runSyncAndGet

// ✅ Always specify condition
userTable.delete(where(ps"id = ${userId}")).runSyncAndGet
```

### Limited Deletion

Delete only a specific number of rows:

```scala
// Delete oldest 100 inactive users
val rowsDeleted = userTable.delete(
  where(ps"active = ${false}")
  orderBy OrderBy("last_login")
  limit 100
).runSyncAndGet

println(s"Deleted $rowsDeleted old inactive users")
```

### Cascading Considerations

sss.db doesn't handle cascading deletes - this is managed by your database:

```sql
-- Define cascading at database level
CREATE TABLE posts (
  id BIGINT PRIMARY KEY,
  user_id BIGINT,
  title VARCHAR(512),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
)
```

```scala
// Deleting user will cascade to posts (if configured in DB)
userTable.delete(where(ps"id = ${userId}")).runSyncAndGet
```

**If no cascade configured, handle manually:**

```scala
// Manual cascade delete in transaction
val transaction = for {
  // Delete posts first
  _ <- postTable.delete(where(ps"user_id = ${userId}"))

  // Then delete user
  _ <- userTable.delete(where(ps"id = ${userId}"))
} yield ()

transaction.runSync
```

### Soft Deletes

Instead of deleting, mark as deleted:

```scala
// Soft delete pattern
userTable.update(
  Map("deleted_at" -> System.currentTimeMillis()),
  where(ps"id = ${userId}")
).runSyncAndGet

// Query only non-deleted
val activeUsers = userTable.filter(
  where("deleted_at IS NULL")
).runSyncAndGet
```

---

## Blob Handling

### ⚠️ Critical: Extract Blobs INSIDE Transactions

When working with blobs (byte arrays, binary data), you **must** extract the data while the transaction is active:

**❌ WRONG - Blob extracted outside transaction:**

```scala
// Fetch row (transaction completes)
val row = blobTable.find(where(ps"id = ${id}")).runSyncAndGet.get

// Try to extract blob (TOO LATE - transaction already closed!)
val data = row.blobByteArray("document")  // May fail or return null!
```

**✅ CORRECT - Blob extracted inside transaction:**

```scala
// Extract inside FutureTx
val blobData: Array[Byte] = (for {
  row <- blobTable.find(where(ps"id = ${id}"))
  data = row.get.blobByteArray("document")  // Extract INSIDE transaction
} yield data).runSyncAndGet
```

**Why this matters:** Blobs are stored as handles that are only valid while the database connection is open. Once the transaction closes, blob handles become invalid.

### Inserting Blobs

```scala
import java.io.{File, FileInputStream}

// From byte array
val imageBytes: Array[Byte] = Files.readAllBytes(Paths.get("image.png"))

blobTable.insert(Map(
  "name" -> "profile.png",
  "content" -> imageBytes
)).runSyncAndGet

// From InputStream
val inputStream = new FileInputStream(new File("document.pdf"))

blobTable.insert(Map(
  "name" -> "document.pdf",
  "content" -> inputStream
)).runSyncAndGet
```

### Reading Blobs

**Option 1: Extract as byte array (inside transaction):**

```scala
val documentBytes: Array[Byte] = (for {
  row <- blobTable.find(where(ps"name = ${"document.pdf"}"))
  bytes = row.get.blobByteArray("content")  // Inside transaction!
} yield bytes).runSyncAndGet

// Now you can use the bytes outside transaction
Files.write(Paths.get("output.pdf"), documentBytes)
```

**Option 2: Extract as InputStream (inside transaction):**

```scala
val inputStream: InputStream = (for {
  row <- blobTable.find(where(ps"id = ${id}"))
  stream = row.get.blobInputStream("content")  // Inside transaction!
} yield stream).runSyncAndGet

// Read from stream
val bytes = inputStream.readAllBytes()
inputStream.close()
```

### Updating Blobs

```scala
val newImageBytes: Array[Byte] = Files.readAllBytes(Paths.get("new_image.png"))

blobTable.update(
  Map("content" -> newImageBytes),
  where(ps"id = ${imageId}")
).runSyncAndGet
```

### freeBlobsEarly Configuration

For large blobs or memory-constrained environments:

```hocon
database {
  datasource {
    # ... connection settings
  }

  # Release blob memory immediately after extraction
  freeBlobsEarly = true  # Default: false
}
```

**When to enable:**
- Working with large blobs (>10MB)
- Memory-constrained environments
- High blob throughput

**Trade-off:** Small performance overhead for memory savings

---

## Batch Operations

### Batch Inserts with FutureTx.sequence

Insert multiple rows in a single transaction:

```scala
val users = List(
  Map("name" -> "Alice", "email" -> "alice@example.com"),
  Map("name" -> "Bob", "email" -> "bob@example.com"),
  Map("name" -> "Charlie", "email" -> "charlie@example.com"),
  Map("name" -> "Dave", "email" -> "dave@example.com")
)

// Create FutureTx for each insert
val insertOps: List[FutureTx[Row]] = users.map { userData =>
  userTable.insert(userData)
}

// Combine into single transaction
val batchInsert: FutureTx[Seq[Row]] = FutureTx.sequence(insertOps)

// Execute all inserts
batchInsert.runSync match {
  case Success(rows) =>
    println(s"✓ Inserted ${rows.length} users")
    rows.foreach { row =>
      println(s"  - ${row.string("name")} (id: ${row.id})")
    }

  case Failure(e) =>
    println(s"✗ Batch insert failed - no users created: ${e.getMessage}")
}
```

**Key points:**
- All inserts execute in **one transaction**
- If any insert fails, **all rollback**
- More efficient than separate transactions

### Batch Updates

```scala
val updates = List(
  (1L, "Alice Smith"),
  (2L, "Bob Jones"),
  (3L, "Charlie Brown")
)

val updateOps: List[FutureTx[Int]] = updates.map { case (id, name) =>
  userTable.update(
    Map("name" -> name),
    where(ps"id = ${id}")
  )
}

val batchUpdate: FutureTx[Seq[Int]] = FutureTx.sequence(updateOps)

batchUpdate.runSync match {
  case Success(counts) =>
    val totalUpdated = counts.sum
    println(s"✓ Updated $totalUpdated rows")

  case Failure(e) =>
    println(s"✗ Batch update failed: ${e.getMessage}")
}
```

### Bulk Insert Pattern

For very large datasets (>1000 rows), consider batching in chunks:

```scala
def bulkInsert(data: Seq[Map[String, Any]], chunkSize: Int = 1000): Unit = {
  data.grouped(chunkSize).foreach { chunk =>
    val insertOps = chunk.map(userTable.insert)
    val batchOp = FutureTx.sequence(insertOps)

    batchOp.runSync match {
      case Success(rows) =>
        println(s"✓ Inserted ${rows.length} rows")

      case Failure(e) =>
        println(s"✗ Chunk failed: ${e.getMessage}")
        throw e
    }
  }
}

// Usage
val largeDataset: Seq[Map[String, Any]] = loadFromFile("users.csv")
bulkInsert(largeDataset, chunkSize = 500)
```

---

## Best Practices

### 1. Use persist() for Idempotent Operations

```scala
// ✅ Good - Idempotent (can run multiple times safely)
userTable.persist(Map(
  "id" -> 123,
  "name" -> "Alice",
  "email" -> "alice@example.com"
)).runSync

// ❌ Bad - Not idempotent (fails on second run)
userTable.insert(Map(
  "id" -> 123,  // Duplicate key error!
  "name" -> "Alice"
)).runSync
```

### 2. Always Include updated_at

```scala
// ✅ Good - Track when row was modified
userTable.update(
  Map(
    "name" -> "Alice Smith",
    "updated_at" -> System.currentTimeMillis()
  ),
  where(ps"id = ${userId}")
).runSyncAndGet
```

### 3. Validate Before Update

```scala
// ✅ Good - Validate in transaction
val transaction = for {
  user <- userTable(userId)

  // Validate
  _ = require(user.boolean("active"), "User is not active")
  _ = require(user.string("email").nonEmpty, "User has no email")

  // Update
  updated <- userTable.update(
    Map("last_login" -> System.currentTimeMillis()),
    where(ps"id = ${userId}")
  )
} yield updated

transaction.runSync
```

### 4. Use Transactions for Related Operations

```scala
// ✅ Good - Atomic transaction
val transaction = for {
  user <- userTable.insert(Map("name" -> "Alice"))
  profile <- profileTable.insert(Map("user_id" -> user.id))
  _ <- settingsTable.insert(Map("user_id" -> user.id))
} yield (user, profile)

transaction.runSync
// All created or none created

// ❌ Bad - Separate transactions (partial failure possible)
val user = userTable.insert(Map("name" -> "Alice")).runSyncAndGet
val profile = profileTable.insert(Map("user_id" -> user.id)).runSyncAndGet
// If this fails, user exists but no profile!
val settings = settingsTable.insert(Map("user_id" -> user.id)).runSyncAndGet
```

### 5. Handle Blob Extraction Correctly

```scala
// ✅ Good - Extract inside transaction
val imageBytes = (for {
  row <- imageTable(imageId)
  bytes = row.blobByteArray("content")
} yield bytes).runSyncAndGet

// ❌ Bad - Extract outside transaction
val row = imageTable(imageId).runSyncAndGet
val bytes = row.blobByteArray("content")  // May fail!
```

### 6. Check Update/Delete Results

```scala
// ✅ Good - Check how many rows affected
val rowsUpdated = userTable.update(
  Map("verified" -> true),
  where(ps"email = ${email}")
).runSyncAndGet

if (rowsUpdated == 0) {
  println("Warning: No rows were updated - user not found?")
}

// ❌ Bad - Assuming update succeeded
userTable.update(
  Map("verified" -> true),
  where(ps"email = ${email}")
).runSyncAndGet
// Did it work? No way to know!
```

### 7. Use Batch Operations

```scala
// ✅ Good - Single transaction
val inserts = users.map(userTable.insert)
FutureTx.sequence(inserts).runSync

// ❌ Bad - Multiple transactions (slow!)
users.foreach { user =>
  userTable.insert(user).runSync
}
```

---

## Summary

### Key Takeaways

**Create:**
- `insert()`: Explicit new row creation
- `persist()`: Insert or update (upsert)
- `insertNoIdentity()`: For non-auto-increment IDs
- Batch with `FutureTx.sequence()`

**Read:**
- See [Queries guide](queries.md) for comprehensive coverage
- Use type-safe accessors on `Row`

**Update:**
- `update()`: Update with WHERE clause
- `persist()` with ID: Upsert pattern
- Always track `updated_at`
- Check rows affected

**Delete:**
- `delete()`: Delete with WHERE clause
- Always use WHERE (prevent accidental full deletion)
- Consider soft deletes
- Handle cascades manually or at DB level

**Blobs:**
- ⚠️ **Extract INSIDE transactions**
- Use `blobByteArray()` or `blobInputStream()`
- Enable `freeBlobsEarly` for large blobs

**Batch Operations:**
- Use `FutureTx.sequence()` for batches
- All execute in one transaction
- More efficient than separate operations

### Next Steps

- **[Queries](queries.md)** - Master reading data
- **[Transactions](transactions.md)** - Compose CRUD operations safely
- **[Performance](performance.md)** - Optimize CRUD performance
- **[Troubleshooting](troubleshooting.md)** - Common CRUD issues
