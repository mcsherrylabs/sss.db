# sss.db

**Simple, type-safe SQL database access for Scala**

[![Build Status](https://travis-ci.org/mcsherrylabs/sss.db.svg?branch=master)](https://travis-ci.org/mcsherrylabs/sss.db) [![Coverage Status](https://coveralls.io/repos/github/mcsherrylabs/sss.db/badge.svg?branch=master)](https://coveralls.io/github/mcsherrylabs/sss.db?branch=master) [![Maven Central](https://img.shields.io/maven-central/v/com.mcsherrylabs/sss-db_2.13.svg)](https://search.maven.org/artifact/com.mcsherrylabs/sss-db_2.13)

sss.db is a Scala library that provides straightforward SQL database access with a focus on simplicity and type safety. It offers composable transactions through the FutureTx monad, automatic optimistic locking, and a natural DSL for queries—all without complex type-level programming.

## Why sss.db?

**Built for greenfield projects that need database access without the learning curve.**

- **Composable Transactions**: Use for-comprehensions to chain database operations in a single transaction
- **Type-Safe Access**: Compile-time checked queries with prepared statements for SQL injection prevention
- **Automatic Optimistic Locking**: Add a `version` column and get concurrency control for free
- **Production Ready**: Built-in connection pooling (HikariCP), error handling, and both sync/async execution
- **Natural DSL**: Write queries that look like SQL but with type safety and parameter binding

**When to use sss.db:**
- You need straightforward database access for a greenfield Scala application
- You want transaction safety without learning complex abstractions
- You prefer simplicity over extensive type-system features
- You're building REST APIs, batch jobs, or CLI tools that need reliable database access

## Quick Start (5 minutes)

### Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "com.mcsherrylabs" %% "sss-db" % "0.9.57"
```

### Configuration

Create `src/main/resources/application.conf`:

```hocon
database {
  datasource {
    driver = "org.hsqldb.jdbc.JDBCDriver"
    connection = "jdbc:hsqldb:file:./data/mydb"
    user = "SA"
    pass = ""
    maxPoolSize = 10
  }

  viewCachesSize = 100
  useShutdownHook = true
}
```

### Hello World

```scala
import sss.db._
import scala.concurrent.ExecutionContext.Implicits.global

// Create execution context (blocking, for simplicity)
implicit val sync = new SyncRunContext(global)

// Initialize database and table
val db = Db("database")
val userTable = db.table("users")

// Insert a row
val user = userTable.persist(Map(
  "name" -> "Alice",
  "email" -> "alice@example.com",
  "age" -> 30
)).runSyncAndGet

println(s"Created user with id: ${user.id}")

// Query the row
val found = userTable.find(where(ps"name = ${"Alice"}")).runSyncAndGet
found.foreach { row =>
  println(s"Found: ${row.string("name")} (${row.int("age")})")
}
```

**That's it!** You've created a database connection, inserted a row, and queried it with type-safe prepared statements.

## Core Example (10 minutes)

### Transaction Composition

The power of sss.db comes from composing database operations in transactions using for-comprehensions:

```scala
import sss.db._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

implicit val sync = new SyncRunContext(global)

val db = Db("database")
val userTable = db.table("users")
val orderTable = db.table("orders")

// Compose multiple operations in a single transaction
val transaction = for {
  // Create a user
  user <- userTable.persist(Map(
    "name" -> "Bob",
    "email" -> "bob@example.com",
    "active" -> true
  ))

  // Create an order for that user
  order <- orderTable.persist(Map(
    "user_id" -> user.id,
    "total" -> 99.99,
    "status" -> "pending"
  ))

  // Update user's last_order_id
  _ <- userTable.update(
    Map("last_order_id" -> order.id),
    where(ps"id = ${user.id}")
  )
} yield (user, order)

// Execute the transaction
transaction.runSync match {
  case Success((user, order)) =>
    println(s"✓ Created user ${user.id} with order ${order.id}")

  case Failure(e: DbOptimisticLockingException) =>
    println("⚠ Version conflict - retry with fresh data")
    // Implement retry logic with exponential backoff

  case Failure(e: DbException) =>
    println(s"✗ Database error: ${e.getMessage}")
    // Handle recoverable errors (constraint violations, timeouts)

  case Failure(e) =>
    println(s"✗ Unrecoverable error: ${e.getMessage}")
    throw e
}
```

**Key features demonstrated:**
- ✅ Multiple operations compose into a single transaction
- ✅ Automatic rollback if any step fails
- ✅ Type-safe row access (`user.id`, `order.id`)
- ✅ Prepared statements prevent SQL injection (`ps""` interpolator)
- ✅ Structured error handling with exception hierarchy

### Query DSL

Write natural-looking queries with a fluent DSL:

```scala
// Find active users, ordered by creation date, limit 10
val activeUsers = userTable.filter(
  where(ps"active = ${true}")
  orderBy OrderDesc("created_at")
  limit 10
).runSyncAndGet

activeUsers.foreach { user =>
  println(s"${user.string("name")} - ${user.string("email")}")
}

// Batch query with IN clause
val userIds = List(1L, 2L, 3L, 4L, 5L)
val orders = orderTable.filter(
  where("user_id").in(userIds)
).runSyncAndGet

println(s"Found ${orders.length} orders for ${userIds.length} users")
```

### Optimistic Locking

Add a `version` column to your table definition and get automatic optimistic locking:

```sql
CREATE TABLE accounts (
  id BIGINT PRIMARY KEY,
  balance DECIMAL(10, 2),
  version BIGINT  -- sss.db detects this and enables optimistic locking
)
```

```scala
val accountTable = db.table("accounts")

// Read account
val account = accountTable(accountId).runSyncAndGet

// Update will automatically check and increment version
accountTable.persist(Map(
  "id" -> account.id,
  "balance" -> account.double("balance") + 100.0,
  "version" -> account.long("version")  // Will fail if version changed
)).runSync match {
  case Success(_) => println("✓ Balance updated")
  case Failure(e: DbOptimisticLockingException) =>
    println("⚠ Account was modified by another transaction - retry")
}
```

## Features

- **Transaction Support**: Compose operations with FutureTx monad
- **Optimistic Locking**: Automatic when table has `version` column
- **Connection Pooling**: HikariCP with configurable pool sizes
- **Type-Safe Access**: Type-safe row accessors (`.string()`, `.long()`, `.int()`, etc.)
- **Prepared Statements**: SQL injection prevention via `ps""` interpolator
- **Sync & Async**: Both blocking (`runSync`) and non-blocking (`run`) execution
- **Natural DSL**: Fluent API for queries, WHERE clauses, ordering, pagination
- **Production Ready**: Structured error handling, resource cleanup, timeout configuration

## Documentation

📚 **[Full Documentation](docs/README.md)** - Comprehensive guides and API reference

**Getting Started:**
- [Getting Started Guide](docs/getting-started.md) - 30-minute tutorial from zero to first app
- [Core Concepts](docs/core-concepts.md) - Understand FutureTx, RunContext, and the View hierarchy

**Guides:**
- [Transactions](docs/transactions.md) - Composition, error handling, isolation levels
- [Queries](docs/queries.md) - Query DSL, WHERE clauses, prepared statements
- [CRUD Operations](docs/crud-operations.md) - Insert, update, delete, find patterns
- [Configuration](docs/configuration.md) - Database setup, connection pooling, security
- [Performance](docs/performance.md) - N+1 prevention, batching, optimization patterns
- [Testing](docs/testing.md) - How to test your code that uses sss.db
- [Troubleshooting](docs/troubleshooting.md) - Production issues and solutions

**Reference:**
- [API Documentation](docs/api/scaladoc/index.html) - Complete ScalaDoc reference

## Security Note

⚠️ **Never commit database credentials to version control.** Use environment variables:

```hocon
database {
  datasource {
    driver = "org.postgresql.Driver"
    connection = ${DATABASE_URL}      # From environment
    user = ${DATABASE_USER}           # From environment
    pass = ${DATABASE_PASSWORD}       # From environment
  }
}
```

The `ps""` interpolator protects against SQL injection for **values**, but table/column names cannot be parameterized. Validate dynamic identifiers before using them in queries.

## Requirements

- Scala 2.13
- Java 11+
- sbt 1.x

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Links

- [GitHub Repository](https://github.com/mcsherrylabs/sss.db)
- [Issue Tracker](https://github.com/mcsherrylabs/sss.db/issues)
- [Maven Central](https://search.maven.org/artifact/com.mcsherrylabs/sss-db_2.13)
- [Documentation](docs/README.md)

---

**Built with ❤️ for Scala developers who value simplicity and type safety.**
