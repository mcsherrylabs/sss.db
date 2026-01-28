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
# Compile the project
sbt compile

# Run tests
sbt test

# Run a specific test
sbt "testOnly sss.db.DbSpec"

# Package for publishing
sbt clean test publishSigned

# Build without publishing
sbt package
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
table.tx {
  val found = table.find(where(ps"blobVal = $bytes"))
  val bytes = found.get.blobByteArray("blobVal") // MUST be inside tx
}
```

**Optimistic Locking**: Automatic if table definition includes a 'version' column. Updates increment version and fail if version changed.

## Configuration

Database configuration goes in `application.conf` (or test resources):

```
database {
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

## Testing

Tests use ScalaTest with trait-based setup:
- `DbSpecSetup`: Standard FlatSpec setup with beforeEach/afterEach
- `DbSpecQuickSetup`: Exposes db/table/syncRunContext as instance variables
- `AsyncDbSpecSetup`: For AsyncFlatSpec tests

Tests create a `Db` instance from config name "testDb" which uses HSQLDB in-memory database. The `afterEach` hook calls `db.shutdown.runSyncAndGet` to clean up.

## Scala Version and Dependencies

- **Scala**: 2.13.10
- **Java**: 11 (source and target)
- **Key dependencies**:
  - sss-ancillary (logging, config)
  - HikariCP (connection pooling)
  - Apache DBCP2/Pool2 (alternative pooling)
  - ScalaTest (testing)

## Publishing

Published to Maven Central via Sonatype. Publishing requires:
- PGP key configured (see build.sbt line 52)
- SONA_USER and SONA_PASS environment variables
- Tag push triggers GitHub Actions CI/CD workflow

## Code Style Notes

- Uses implicit RunContext for execution (passed to Tables/Views/Queries)
- Extensive use of type aliases (Rows, QueryResults, ColumnTypes)
- Pattern: try/finally for resource cleanup (not Try monad)
- Column names are case-insensitive (converted to lowercase)
- Row accessors have both `.opt` and non-opt versions (throwing on None)
