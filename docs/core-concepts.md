# Core Concepts

This guide explains the architectural abstractions that power sss.db. Understanding these concepts will help you write more sophisticated database applications and make better architectural decisions.

**Prerequisites:** Complete the [Getting Started](getting-started.md) guide first for hands-on experience before diving into these concepts.

## Table of Contents

- [FutureTx[T] - The Central Monad](#futuretxt---the-central-monad)
- [RunContext - Execution Control](#runcontext---execution-control)
- [View Hierarchy - Read/Write Abstractions](#view-hierarchy---readwrite-abstractions)
- [Row - Type-Safe Result Access](#row---type-safe-result-access)
- [Where - Query DSL](#where---query-dsl)
- [Putting It All Together](#putting-it-all-together)

---

## FutureTx[T] - The Central Monad

### What is FutureTx?

`FutureTx[T]` is the heart of sss.db. It's a monad that represents **a database operation to be executed within a transaction**.

**Type signature:**
```scala
trait FutureTx[+T] extends (TransactionContext => Future[T])
```

In plain English: **FutureTx is a function that takes a transaction context (connection + execution context) and returns a Future containing the result.**

### Why FutureTx Matters

FutureTx gives you three critical properties:

1. **Composability**: Combine multiple operations into a single transaction using for-comprehensions
2. **Laziness**: Operations don't execute until you call `.run` or `.runSync`
3. **Automatic Transaction Management**: Connection acquisition, commit, rollback, and cleanup happen automatically

### FutureTx vs. Raw JDBC

Let's compare sss.db with raw JDBC to understand the value:

**Raw JDBC (the old way):**

```scala
val conn = dataSource.getConnection()
conn.setAutoCommit(false)

try {
  // Insert user
  val insertUserStmt = conn.prepareStatement(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  insertUserStmt.setString(1, "Alice")
  insertUserStmt.setString(2, "alice@example.com")
  insertUserStmt.executeUpdate()

  val userRs = insertUserStmt.getGeneratedKeys()
  userRs.next()
  val userId = userRs.getLong(1)
  userRs.close()
  insertUserStmt.close()

  // Insert post
  val insertPostStmt = conn.prepareStatement(
    "INSERT INTO posts (user_id, title) VALUES (?, ?)"
  )
  insertPostStmt.setLong(1, userId)
  insertPostStmt.setString(2, "My First Post")
  insertPostStmt.executeUpdate()
  insertPostStmt.close()

  conn.commit()

} catch {
  case e: Exception =>
    conn.rollback()
    throw e
} finally {
  conn.close()
}
```

**sss.db with FutureTx (the new way):**

```scala
val transaction = for {
  user <- userTable.persist(Map("name" -> "Alice", "email" -> "alice@example.com"))
  post <- postTable.persist(Map("user_id" -> user.id, "title" -> "My First Post"))
} yield (user, post)

transaction.runSync  // Connection, commit, rollback, cleanup all automatic
```

**What sss.db handles for you:**
- ✅ Connection acquisition and release
- ✅ Prepared statement creation and parameter binding
- ✅ Result set extraction
- ✅ Automatic commit on success
- ✅ Automatic rollback on exception
- ✅ Resource cleanup (always runs, even on exception)
- ✅ Type-safe result access

### Monadic Composition

FutureTx is a monad, which means it supports `map` and `flatMap`. This enables for-comprehension syntax:

```scala
// Using for-comprehension (recommended)
val result = for {
  user <- userTable.persist(Map("name" -> "Bob"))
  posts <- postTable.filter(where(ps"user_id = ${user.id}"))
} yield (user, posts)

// Equivalent using flatMap and map (what for-comprehension desugars to)
val result = userTable.persist(Map("name" -> "Bob")).flatMap { user =>
  postTable.filter(where(ps"user_id = ${user.id}")).map { posts =>
    (user, posts)
  }
}
```

**Key insight:** Each step in a for-comprehension can depend on the results of previous steps, and they all execute in a single transaction.

### Lazy Execution

FutureTx operations are **lazy** - they don't execute until you explicitly run them:

```scala
// This DOES NOT execute - just builds the operation
val operation = userTable.persist(Map("name" -> "Charlie"))

// Still not executed - just transformed the operation
val transformed = operation.map(user => user.string("name"))

// NOW it executes (and returns Try[String])
val result = transformed.runSync
```

**Why laziness matters:**
- You can build complex operations programmatically before executing
- Operations compose efficiently without intermediate execution
- You control exactly when side effects happen

### FutureTx Constructors

Create FutureTx values from scratch:

```scala
import sss.db.FutureTx

// Successful value
val success: FutureTx[String] = FutureTx.unit("Hello")

// Failed value
val failure: FutureTx[String] = FutureTx.failed(new Exception("Oops"))

// Lazy evaluation (evaluated when transaction runs)
val lazy: FutureTx[String] = FutureTx.lazyUnit {
  println("This prints when transaction executes")
  "Computed value"
}
```

### Batching with FutureTx.sequence

Execute multiple operations in parallel within a transaction:

```scala
val users = List("Alice", "Bob", "Charlie")

// Create a FutureTx for each user
val insertOps: List[FutureTx[Row]] = users.map { name =>
  userTable.persist(Map("name" -> name))
}

// Combine into single transaction
val batchOp: FutureTx[Seq[Row]] = FutureTx.sequence(insertOps)

// Execute all inserts in one transaction
batchOp.runSync match {
  case Success(rows) => println(s"Inserted ${rows.length} users")
  case Failure(e) => println(s"Batch insert failed: ${e.getMessage}")
}
```

**Important:** All operations in `sequence` execute in a **single transaction**. If any operation fails, all changes roll back.

---

## RunContext - Execution Control

`RunContext` controls **how** FutureTx operations execute: synchronously (blocking) or asynchronously (non-blocking).

### Two Implementations

```
RunContext (trait)
    ├── SyncRunContext  (blocking execution)
    └── AsyncRunContext (non-blocking execution)
```

### SyncRunContext: Blocking Execution

**Use for:** CLIs, batch jobs, simple scripts, low-concurrency applications (<10 req/sec)

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// Create sync context
implicit val sync = new SyncRunContext(global)

// Execute with .runSync (returns Try[T])
val result: Try[Row] = userTable.persist(Map("name" -> "Alice")).runSync

result match {
  case Success(row) => println(s"Created: ${row.id}")
  case Failure(e) => println(s"Failed: ${e.getMessage}")
}

// Execute with .runSyncAndGet (returns T or throws)
val row: Row = userTable.persist(Map("name" -> "Bob")).runSyncAndGet
```

**Characteristics:**
- **Blocks the calling thread** until operation completes
- Returns `Try[T]` (`.runSync`) or `T` (`.runSyncAndGet`)
- Simple mental model - operations complete before moving on
- Default timeout: 3 seconds (configurable)

**Timeout Configuration:**

```scala
import scala.concurrent.duration._

implicit val sync = new SyncRunContext(global, timeout = 30.seconds)
```

### AsyncRunContext: Non-Blocking Execution

**Use for:** Web services, high-concurrency apps (>100 req/sec), when you need efficient thread utilization

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// Create async context
implicit val async = new AsyncRunContext(global)

// Execute with .run (returns Future[T])
val futureRow: Future[Row] = userTable.persist(Map("name" -> "Alice")).run

futureRow.map { row =>
  println(s"Created: ${row.id}")
}.recover {
  case e: DbException => println(s"Failed: ${e.getMessage}")
}
```

**Characteristics:**
- **Non-blocking** - calling thread immediately continues
- Returns `Future[T]`
- More efficient thread utilization (futures queue while waiting for connections)
- Requires understanding of Future composition

**Integration with Web Frameworks:**

```scala
// Play Framework
def createUser = Action.async { implicit request =>
  val form = userForm.bindFromRequest

  userTable.persist(Map(
    "name" -> form.name,
    "email" -> form.email
  )).run.map { user =>
    Ok(Json.toJson(user))
  }.recover {
    case e: DbException => BadRequest(e.getMessage)
  }
}

// Akka HTTP
val route = post {
  entity(as[UserData]) { userData =>
    val futureUser = userTable.persist(Map(
      "name" -> userData.name,
      "email" -> userData.email
    )).run

    onComplete(futureUser) {
      case Success(user) => complete(StatusCodes.Created, user)
      case Failure(e) => complete(StatusCodes.BadRequest, e.getMessage)
    }
  }
}
```

### When to Use Each

| Scenario | Use SyncRunContext | Use AsyncRunContext |
|----------|-------------------|---------------------|
| CLI tool | ✅ Simple blocking | ❌ Overkill |
| Batch job | ✅ Sequential processing | ⚠️ Only if parallelizing |
| REST API (<10 req/sec) | ✅ Simpler code | ⚠️ Adds complexity |
| REST API (>100 req/sec) | ❌ Thread starvation | ✅ Better throughput |
| WebSocket server | ❌ Blocks threads | ✅ Non-blocking |
| Microservice | ⚠️ Depends on load | ✅ Recommended |

**Rule of thumb:** If you need the result immediately and concurrency is low, use sync. For high throughput, use async.

### Thread Pool Considerations

**SyncRunContext:**
- Blocks one thread per concurrent database operation
- Pool size recommendation: `number_of_concurrent_transactions`
- Example: If you expect 10 concurrent requests, your thread pool should have ~10 threads

**AsyncRunContext:**
- Futures queue efficiently while waiting for database connections
- Can use smaller thread pool than number of concurrent operations
- Pool size recommendation: `number_of_CPU_cores * 2`

---

## View Hierarchy - Read/Write Abstractions

sss.db provides different abstractions depending on what operations you need. This hierarchy lets you express intent clearly and prevents accidental misuse.

### The Hierarchy

```
Query (base class)
  │
  └─ View (read-only: count, max, filter, find)
       │
       ├─ UpdatableView (adds: update, delete)
       │
       └─ InsertableView (adds: insert, persist)
              │
              └─ Table (full access: all operations)
```

**Visualization:**

```
Operations Available:
                              Query  View  UpdatableView  InsertableView  Table
SELECT (filter, find, etc.)     ✅     ✅        ✅             ✅          ✅
COUNT, MAX                      ❌     ✅        ✅             ✅          ✅
UPDATE                          ❌     ❌        ✅             ❌          ✅
DELETE                          ❌     ❌        ✅             ❌          ✅
INSERT                          ❌     ❌        ❌             ✅          ✅
PERSIST (upsert)                ❌     ❌        ❌             ✅          ✅
```

### Query (Base Class)

The foundational class for SELECT operations with DSL support.

```scala
// Usually don't use Query directly - use View or Table
class Query[T](
  tableName: String,
  where: Where,
  runContext: RunContext,
  freeBlobsEarly: Boolean,
  cols: String = "*"
)
```

**Key operations:**
- `filter(where: Where): FutureTx[Seq[Row]]`
- `find(where: Where): FutureTx[Option[Row]]`
- `map[T](f: Row => T): FutureTx[Seq[T]]`

### View (Read-Only)

A read-only view of a database table or SQL view. Use when you only need to query data.

```scala
val db = Db("database")
val userView = db.view("users")  // Read-only

// Available operations
val count = userView.count(where(ps"active = ${true}")).runSyncAndGet
val maxAge = userView.max("age").runSyncAndGet
val users = userView.filter(where(ps"age > ${25}")).runSyncAndGet

// NOT available (compile error)
// userView.update(...)  // ❌ Compile error
// userView.delete(...)  // ❌ Compile error
```

**When to use View:**
- Reading from SQL views (not tables)
- Enforcing read-only access in your domain model
- Querying data without modification

**Creating a View with custom columns:**

```scala
// Only fetch specific columns (performance optimization)
val userView = new View(
  tableName = "users",
  where = Where(),
  runContext = db.syncRunContext,
  freeBlobsEarly = false,
  cols = "id, name, email"  // Only these columns
)
```

### UpdatableView (Read + Update + Delete)

Extends View with update and delete operations.

```scala
val db = Db("database")
val updatableUsers = db.updatableView("users")

// Read operations (from View)
val users = updatableUsers.findAll().runSyncAndGet

// Update operations
updatableUsers.update(
  Map("active" -> false),
  where(ps"last_login < ${thirtyDaysAgo}")
).runSyncAndGet

// Delete operations
updatableUsers.delete(
  where(ps"active = ${false}")
).runSyncAndGet

// NOT available (compile error)
// updatableUsers.persist(...)  // ❌ No insert operations
```

**When to use UpdatableView:**
- When you need to update/delete but not insert
- Implementing command patterns (updates only)
- Working with views that support updates (rare)

### InsertableView (Read + Insert)

Extends View with insert and persist (upsert) operations.

```scala
val db = Db("database")
val insertableUsers = db.insertableView("users")

// Read operations (from View)
val users = insertableUsers.findAll().runSyncAndGet

// Insert operations
val newUser = insertableUsers.insert(Map(
  "name" -> "Alice",
  "email" -> "alice@example.com"
)).runSyncAndGet

// Persist (upsert: insert or update if exists)
val user = insertableUsers.persist(Map(
  "id" -> 123,
  "name" -> "Bob",
  "email" -> "bob@example.com"
)).runSyncAndGet

// NOT available (compile error)
// insertableUsers.update(...)  // ❌ No update operations
// insertableUsers.delete(...)  // ❌ No delete operations
```

**When to use InsertableView:**
- Append-only tables (no updates/deletes)
- Event sourcing patterns
- Audit logs

### Table (Full Access)

The most common abstraction - provides all operations.

```scala
val db = Db("database")
val userTable = db.table("users")

// All operations available
val users = userTable.findAll().runSyncAndGet             // Read
val count = userTable.count().runSyncAndGet                // Aggregate
val user = userTable.persist(Map("name" -> "Alice")).runSyncAndGet  // Insert
userTable.update(Map("active" -> true), where("id" -> 1)).runSyncAndGet  // Update
userTable.delete(where("id" -> 1)).runSyncAndGet          // Delete
```

**When to use Table:**
- Most use cases (full CRUD access)
- Working with actual database tables
- When you need all operations

### Choosing the Right Abstraction

**Design principle:** Use the most restrictive abstraction that fits your needs. This makes code intent clear and prevents accidental misuse.

```scala
// Domain model example
class UserRepository(db: Db) {
  // Public API - read-only
  val users: View = db.view("users")

  // Internal use - full access
  private val userTable: Table = db.table("users")

  def findById(id: Long): FutureTx[Option[Row]] = {
    users.find(where("id" -> id))  // Can't accidentally modify
  }

  def createUser(data: Map[String, Any]): FutureTx[Row] = {
    userTable.persist(data)  // Full access for internal use
  }
}
```

---

## Row - Type-Safe Result Access

`Row` represents a single row from a query result with type-safe accessors.

### Type-Safe Accessors

Instead of casting or using generics, Row provides dedicated methods for each type:

```scala
val user = userTable.find(where("id" -> 1)).runSyncAndGet.get

// String columns
val name: String = user.string("name")
val email: String = user.string("email")

// Numeric columns
val age: Int = user.int("age")
val id: Long = user.long("id")
val salary: Double = user.double("salary")
val score: Float = user.float("score")
val count: Short = user.short("count")

// Boolean columns
val active: Boolean = user.boolean("active")

// Date/Time columns
val createdAt: java.sql.Timestamp = user.timestamp("created_at")
val birthDate: java.sql.Date = user.date("birth_date")

// Binary data
val avatar: Array[Byte] = user.byteArray("avatar")
val document: java.sql.Blob = user.blob("document")
```

### Column Name Normalization

**Important:** Column names are case-insensitive and normalized to lowercase:

```scala
// These are all equivalent
user.string("name")
user.string("NAME")
user.string("Name")
user.string("NaMe")
```

**Why?** Different databases handle case differently. Normalization ensures consistent behavior.

### Auto-Generated ID Access

Every Row has an `.id` property that accesses the primary key:

```scala
val user = userTable.persist(Map("name" -> "Alice")).runSyncAndGet

// These are equivalent (assuming primary key column is "id")
val userId1: Long = user.id
val userId2: Long = user.long("id")
```

### Working with Nullable Columns

For nullable columns, use `Option` types:

```scala
val middleName: Option[String] = user.stringOpt("middle_name")
val age: Option[Int] = user.intOpt("age")

middleName match {
  case Some(name) => println(s"Middle name: $name")
  case None => println("No middle name")
}

// Or use getOrElse
val displayName = user.stringOpt("nickname").getOrElse(user.string("name"))
```

### Converting Row to Map

Extract all columns as a Map:

```scala
val user = userTable.find(where("id" -> 1)).runSyncAndGet.get

// Get all columns as Map[String, Any]
val dataMap: Map[String, Any] = user.asMap

// Useful for updates
val updatedData = dataMap + ("age" -> 31)
userTable.persist(updatedData).runSyncAndGet
```

### Deprecated: apply[T] Method

⚠️ **Deprecated:** The generic `apply[T]()` method is deprecated in favor of specific typed methods:

```scala
// ❌ Deprecated (don't use)
val name = user[String]("name")
val age = user[Int]("age")

// ✅ Use instead
val name = user.string("name")
val age = user.int("age")
```

**Why deprecated?** Specific methods provide better type safety and clearer intent.

---

## Where - Query DSL

`Where` provides a fluent DSL for building WHERE clauses with proper SQL injection prevention.

### Basic WHERE Clauses

```scala
import sss.db._

// Simple equality
val users = userTable.filter(
  where("active" -> true)
).runSyncAndGet

// Prepared statement interpolator (recommended for values)
val users = userTable.filter(
  where(ps"age > ${25}")
).runSyncAndGet

// Multiple columns
val users = userTable.filter(
  where("active" -> true, "age" -> 30)
).runSyncAndGet
```

### Method Chaining

Chain methods to build complex queries:

```scala
import sss.db._

val users = userTable.filter(
  where(ps"age > ${25}")
  and(ps"active = ${true}")
  orderBy OrderDesc("created_at")
  limit 10
  offset 5
).runSyncAndGet
```

**Available methods:**
- `and(condition: String, values: Any*): Where` - Add AND condition
- `orderBy(order: Order): Where` - Add ordering
- `limit(n: Int): Where` - Limit results
- `offset(n: Int): Where` - Skip results
- `in(values: Iterable[_]): Where` - IN clause
- `notIn(values: Iterable[_]): Where` - NOT IN clause

### Ordering

```scala
import sss.db._

// Ascending order
val users = userTable.filter(
  where() orderBy OrderBy("name")
).runSyncAndGet

// Descending order
val users = userTable.filter(
  where() orderBy OrderDesc("created_at")
).runSyncAndGet

// Multiple columns
val users = userTable.filter(
  where()
  orderBy OrderBy("last_name")
  orderBy OrderBy("first_name")
).runSyncAndGet
```

### IN and NOT IN Clauses

```scala
// IN clause
val userIds = List(1L, 2L, 3L, 4L, 5L)
val users = userTable.filter(
  where("id").in(userIds)
).runSyncAndGet

// NOT IN clause
val excludedIds = Set(10L, 20L, 30L)
val users = userTable.filter(
  where("id").notIn(excludedIds)
).runSyncAndGet
```

### Prepared Statement Interpolator (ps"")

**Always use `ps""` for values** to prevent SQL injection:

```scala
import sss.db._

// ✅ Safe - values are parameterized
val email = "alice@example.com"
val users = userTable.filter(
  where(ps"email = $email")
).runSyncAndGet

// ✅ Safe - multiple parameters
val minAge = 25
val maxAge = 35
val users = userTable.filter(
  where(ps"age >= $minAge AND age <= $maxAge")
).runSyncAndGet
```

### SQL Injection Prevention

⚠️ **Security Warning:** The `ps""` interpolator protects **values** but not table/column names.

```scala
// ✅ SAFE - value is parameterized
val userInput = "alice@example.com"
userTable.filter(where(ps"email = $userInput"))

// ❌ UNSAFE - column name cannot be parameterized
val columnName = userInput  // If this comes from user, it's vulnerable!
userTable.filter(where(s"$columnName = ?", value))

// ✅ SAFE - validate column names
val allowedColumns = Set("id", "name", "email", "age")
require(allowedColumns.contains(columnName), s"Invalid column: $columnName")
userTable.filter(where(s"$columnName = ?", value))
```

**Key rule:** Never interpolate user input into column/table names without validation.

---

## Putting It All Together

Let's build a complete example using all these concepts:

```scala
package example

import sss.db._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import java.util.Date

object CompleteConceptsExample extends App {

  // 1. RunContext - Choose execution strategy
  implicit val sync = new SyncRunContext(global)

  // 2. Initialize database
  val db = Db("database")

  // 3. View Hierarchy - Choose appropriate abstractions
  val userTable: Table = db.table("users")              // Full access
  val userView: View = db.view("users")                 // Read-only
  val postTable: Table = db.table("posts")

  // 4. FutureTx - Compose operations in transaction
  val transaction: FutureTx[(Row, Row, Seq[Row])] = for {
    // Create user
    user <- userTable.persist(Map(
      "name" -> "Alice",
      "email" -> "alice@example.com",
      "age" -> 30,
      "active" -> true,
      "created_at" -> new Date().getTime
    ))

    // Create post for user
    post <- postTable.persist(Map(
      "user_id" -> user.id,               // Row - type-safe access
      "title" -> "Understanding sss.db",
      "content" -> "Core concepts explained",
      "published" -> true
    ))

    // Query using Where DSL
    allPosts <- postTable.filter(
      where(ps"user_id = ${user.id}")     // Where - prepared statements
      orderBy OrderDesc("created_at")
      limit 10
    )

  } yield (user, post, allPosts)

  // Execute transaction
  transaction.runSync match {
    case Success((user, post, allPosts)) =>
      println("✓ Transaction succeeded!")
      println(s"  User: ${user.string("name")} (id: ${user.id})")
      println(s"  Post: ${post.string("title")} (id: ${post.id})")
      println(s"  Found ${allPosts.length} posts for user")

    case Failure(e: DbOptimisticLockingException) =>
      println("⚠ Optimistic locking conflict")

    case Failure(e: DbException) =>
      println(s"✗ Database error: ${e.getMessage}")

    case Failure(e) =>
      println(s"✗ Fatal error: ${e.getMessage}")
      throw e
  }

  // Cleanup
  db.shutdown.runSyncAndGet
}
```

**This example demonstrates:**
1. ✅ **RunContext**: Choosing sync execution
2. ✅ **View Hierarchy**: Using Table and View appropriately
3. ✅ **FutureTx**: Composing three operations in a transaction
4. ✅ **Row**: Type-safe access (`.id`, `.string()`)
5. ✅ **Where**: Building queries with `ps""` interpolator
6. ✅ **Error Handling**: Pattern matching on exception types

---

## Summary

### Key Takeaways

**FutureTx[T]:**
- Represents database operations to execute in a transaction
- Lazy (doesn't execute until `.run` or `.runSync`)
- Composable (use for-comprehensions)
- Handles transactions automatically

**RunContext:**
- `SyncRunContext`: Blocking execution (CLI, batch jobs)
- `AsyncRunContext`: Non-blocking execution (web services)

**View Hierarchy:**
- `View`: Read-only
- `UpdatableView`: Read + update + delete
- `InsertableView`: Read + insert
- `Table`: Full access (most common)

**Row:**
- Type-safe accessors (`.string()`, `.int()`, `.long()`)
- Case-insensitive column names
- `.id` for primary key access

**Where:**
- Fluent DSL for WHERE clauses
- Method chaining (`.and()`, `.orderBy()`, `.limit()`)
- `ps""` interpolator prevents SQL injection

### Next Steps

Now that you understand the core concepts, explore advanced topics:

- **[Transactions](transactions.md)** - Deep dive into transaction patterns, isolation levels, and optimistic locking
- **[Queries](queries.md)** - Master the query DSL and advanced filtering
- **[CRUD Operations](crud-operations.md)** - Comprehensive guide to all CRUD patterns
- **[Performance](performance.md)** - Optimize your database access

### Further Reading

- **[API Documentation](api/scaladoc/index.html)** - Complete ScalaDoc reference
- **[Configuration](configuration.md)** - Connection pooling and database setup
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
