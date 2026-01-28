# Queries

This guide covers everything you need to know about querying data with sss.db's natural DSL. You'll learn how to safely construct WHERE clauses, handle complex filters, and avoid common pitfalls like SQL injection.

**Prerequisites:** Read [Getting Started](getting-started.md) and [Core Concepts](core-concepts.md) first.

## Table of Contents

- [Basic Queries](#basic-queries)
- [WHERE Clauses](#where-clauses)
- [Method Chaining](#method-chaining)
- [IN and NOT IN Clauses](#in-and-not-in-clauses)
- [Ordering and Pagination](#ordering-and-pagination)
- [SQL Injection Prevention](#sql-injection-prevention)
- [Advanced Patterns](#advanced-patterns)
- [Performance Considerations](#performance-considerations)

---

## Basic Queries

### find - Single Result

Returns `Option[Row]` - `Some(row)` if found, `None` if not found:

```scala
val db = Db("database")
val userTable = db.table("users")

// Find by primary key
val user: Option[Row] = userTable.find(where("id" -> 1)).runSyncAndGet

user match {
  case Some(row) => println(s"Found: ${row.string("name")}")
  case None => println("User not found")
}

// Find by other column
val user: Option[Row] = userTable.find(
  where(ps"email = ${"alice@example.com"}")
).runSyncAndGet
```

### filter - Multiple Results

Returns `Seq[Row]` - all matching rows:

```scala
// Find all active users
val activeUsers: Seq[Row] = userTable.filter(
  where(ps"active = ${true}")
).runSyncAndGet

println(s"Found ${activeUsers.length} active users")

activeUsers.foreach { user =>
  println(s"- ${user.string("name")} (${user.string("email")})")
}
```

### findAll - All Rows

Returns all rows in the table (no WHERE clause):

```scala
val allUsers: Seq[Row] = userTable.findAll().runSyncAndGet

println(s"Total users: ${allUsers.length}")
```

**⚠️ Warning:** `findAll()` loads all rows into memory. For large tables (>10,000 rows), use [PagedView](#paged-view-for-large-result-sets) instead.

### get vs apply

Two ways to fetch by primary key:

```scala
// get() returns Option[Row]
val user: FutureTx[Option[Row]] = userTable.get(123)

user.runSyncAndGet match {
  case Some(row) => println(s"Found: ${row.string("name")}")
  case None => println("Not found")
}

// apply() returns Row (throws if not found)
val user: FutureTx[Row] = userTable(123)

val row = user.runSyncAndGet  // Throws exception if id=123 doesn't exist
println(s"Found: ${row.string("name")}")
```

**When to use each:**
- Use `get()` when the row might not exist (returns `Option`)
- Use `apply()` when you know the row exists (throws exception if not)

### count - Count Rows

Count rows matching a condition:

```scala
// Count all rows
val totalUsers: Long = userTable.count().runSyncAndGet
println(s"Total users: $totalUsers")

// Count with condition
val activeCount: Long = userTable.count(
  where(ps"active = ${true}")
).runSyncAndGet
println(s"Active users: $activeCount")
```

### max - Maximum Value

Get the maximum value of a column:

```scala
// Get oldest user's age
val maxAge: Option[Int] = userTable.max("age").runSyncAndGet

maxAge match {
  case Some(age) => println(s"Oldest user is $age years old")
  case None => println("No users found")
}

// Get highest order total
val maxTotal: Option[Double] = orderTable.max("total").runSyncAndGet
```

**Note:** Returns `Option` because the table might be empty.

---

## WHERE Clauses

### Simple Equality

```scala
// Single column
val users = userTable.filter(
  where("active" -> true)
).runSyncAndGet

// Multiple columns (AND condition)
val users = userTable.filter(
  where("active" -> true, "age" -> 30)
).runSyncAndGet
// Generates: WHERE active = true AND age = 30
```

### Prepared Statement Interpolator (ps"")

**Always use `ps""` for dynamic values** to prevent SQL injection:

```scala
import sss.db._

val email = "alice@example.com"
val minAge = 25

// ✅ Safe - values are parameterized
val users = userTable.filter(
  where(ps"email = $email")
).runSyncAndGet

// ✅ Safe - multiple parameters
val users = userTable.filter(
  where(ps"age >= $minAge AND active = ${true}")
).runSyncAndGet
```

**What `ps""` does:**
- Creates a prepared statement with `?` placeholders
- Binds values safely (prevents SQL injection)
- Handles type conversion automatically

### Comparison Operators

```scala
// Greater than
val users = userTable.filter(
  where(ps"age > ${25}")
).runSyncAndGet

// Less than or equal
val users = userTable.filter(
  where(ps"age <= ${40}")
).runSyncAndGet

// Not equal
val users = userTable.filter(
  where(ps"status != ${"banned"}")
).runSyncAndGet

// Between (use AND)
val users = userTable.filter(
  where(ps"age >= ${25} AND age <= ${40}")
).runSyncAndGet
```

### LIKE Pattern Matching

```scala
val searchTerm = "alice"

// LIKE with wildcards
val users = userTable.filter(
  where(ps"name LIKE ${s"%$searchTerm%"}")  // Contains
).runSyncAndGet

// Starts with
val users = userTable.filter(
  where(ps"email LIKE ${s"$searchTerm%"}")  // alice%
).runSyncAndGet

// Ends with
val users = userTable.filter(
  where(ps"email LIKE ${s"%$searchTerm"}")  // %alice
).runSyncAndGet
```

### IS NULL / IS NOT NULL

```scala
// IS NULL
val users = userTable.filter(
  where("middle_name IS NULL")
).runSyncAndGet

// IS NOT NULL
val users = userTable.filter(
  where("email IS NOT NULL")
).runSyncAndGet
```

**Note:** `IS NULL` cannot use prepared statements (it's a keyword, not a value).

---

## Method Chaining

Build complex queries by chaining methods on `Where`:

### and() - Add Conditions

```scala
val users = userTable.filter(
  where(ps"age > ${25}")
  and(ps"active = ${true}")
  and(ps"email IS NOT NULL")
).runSyncAndGet

// Generates: WHERE age > ? AND active = ? AND email IS NOT NULL
```

### Multiple Filters

```scala
val minAge = 25
val maxAge = 40
val city = "New York"

val users = userTable.filter(
  where(ps"age >= $minAge")
  and(ps"age <= $maxAge")
  and(ps"city = $city")
  and("verified = true")
).runSyncAndGet
```

### Conditional Filters

Build queries dynamically based on conditions:

```scala
def searchUsers(
  minAge: Option[Int],
  city: Option[String],
  activeOnly: Boolean
): Seq[Row] = {

  var query = where("")

  // Add age filter if provided
  minAge.foreach { age =>
    query = query.and(ps"age >= $age")
  }

  // Add city filter if provided
  city.foreach { c =>
    query = query.and(ps"city = $c")
  }

  // Add active filter
  if (activeOnly) {
    query = query.and(ps"active = ${true}")
  }

  userTable.filter(query).runSyncAndGet
}

// Usage
searchUsers(minAge = Some(25), city = Some("NYC"), activeOnly = true)
searchUsers(minAge = None, city = Some("LA"), activeOnly = false)
```

---

## IN and NOT IN Clauses

### IN Clause

Match any value in a collection:

```scala
val userIds = List(1L, 2L, 3L, 4L, 5L)

val users = userTable.filter(
  where("id").in(userIds)
).runSyncAndGet

// Generates: WHERE id IN (1, 2, 3, 4, 5)
```

**Works with any collection:**

```scala
// Set
val statusSet = Set("active", "pending", "verified")
val users = userTable.filter(
  where("status").in(statusSet)
).runSyncAndGet

// Seq
val emailList = Seq("alice@example.com", "bob@example.com")
val users = userTable.filter(
  where("email").in(emailList)
).runSyncAndGet
```

### NOT IN Clause

Exclude values:

```scala
val excludedIds = Set(10L, 20L, 30L)

val users = userTable.filter(
  where("id").notIn(excludedIds)
).runSyncAndGet

// Generates: WHERE id NOT IN (10, 20, 30)
```

### Combining IN with Other Conditions

```scala
val userIds = List(1L, 2L, 3L, 4L, 5L)

val users = userTable.filter(
  where("id").in(userIds)
  and(ps"active = ${true}")
  and(ps"age > ${25}")
).runSyncAndGet

// Generates: WHERE id IN (1, 2, 3, 4, 5) AND active = ? AND age > ?
```

### Empty Collections

**Important:** IN clauses with empty collections return no results:

```scala
val emptyList = List.empty[Long]

val users = userTable.filter(
  where("id").in(emptyList)
).runSyncAndGet

// Returns empty Seq (WHERE id IN () matches nothing)
```

**Handle empty collections:**

```scala
def findUsersByIds(ids: List[Long]): Seq[Row] = {
  if (ids.isEmpty) {
    Seq.empty  // Return early
  } else {
    userTable.filter(where("id").in(ids)).runSyncAndGet
  }
}
```

---

## Ordering and Pagination

### orderBy - Sort Results

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
```

### Multiple Order Columns

```scala
// Order by last name, then first name
val users = userTable.filter(
  where()
  orderBy OrderBy("last_name")
  orderBy OrderBy("first_name")
).runSyncAndGet

// Generates: ORDER BY last_name, first_name
```

### Combining Filters and Ordering

```scala
val users = userTable.filter(
  where(ps"age > ${25}")
  and(ps"active = ${true}")
  orderBy OrderDesc("created_at")
).runSyncAndGet
```

### limit - Limit Results

```scala
// Get top 10 users
val users = userTable.filter(
  where()
  orderBy OrderDesc("score")
  limit 10
).runSyncAndGet
```

### offset - Skip Results

```scala
// Skip first 10 results (pagination)
val users = userTable.filter(
  where()
  orderBy OrderBy("id")
  limit 10
  offset 10
).runSyncAndGet

// Generates: ... ORDER BY id LIMIT 10 OFFSET 10
```

### Simple Pagination

```scala
def getPage(pageNumber: Int, pageSize: Int): Seq[Row] = {
  val offset = pageNumber * pageSize

  userTable.filter(
    where()
    orderBy OrderBy("id")
    limit pageSize
    offset offset
  ).runSyncAndGet
}

// Usage
val page0 = getPage(0, 10)  // First 10 users
val page1 = getPage(1, 10)  // Next 10 users
val page2 = getPage(2, 10)  // Next 10 users
```

### page() Method

Convenience method for pagination:

```scala
// Get specific page
val page1: Seq[Row] = userTable.page(start = 0, pageSize = 10).runSyncAndGet
val page2: Seq[Row] = userTable.page(start = 10, pageSize = 10).runSyncAndGet

// With filters
val activeUsersPage1 = userTable.filter(
  where(ps"active = ${true}")
).page(start = 0, pageSize = 10).runSyncAndGet
```

### Paged View for Large Result Sets

For tables with >10,000 rows, use `PagedView` to avoid loading everything into memory:

```scala
// Convert table to paged view
val pagedView: PagedView = userTable.toPaged(pageSize = 1000)

// Iterate through pages (only loads 1000 rows at a time)
pagedView.toIterator.foreach { row =>
  println(s"Processing: ${row.string("name")}")
  // Only 1000 rows in memory at any time
}

// Convert to lazy stream
val stream: LazyList[Row] = pagedView.toStream

stream.take(5000).foreach { row =>
  processUser(row)
  // Fetches pages on-demand
}
```

**When to use PagedView:**
- Tables with >10,000 rows
- Batch processing entire tables
- Memory-constrained environments
- Streaming/pipeline architectures

---

## SQL Injection Prevention

### What's Safe, What's Not

sss.db uses prepared statements for **values**, which prevents SQL injection. However, table names, column names, and SQL keywords **cannot be parameterized**.

**✅ Safe (parameterized values):**

```scala
val userInput = "alice@example.com"
val statusInput = "active"

// Values are parameterized
userTable.filter(
  where(ps"email = $userInput AND status = $statusInput")
).runSyncAndGet

// Generates: WHERE email = ? AND status = ?
// Parameters: ["alice@example.com", "active"]
```

**❌ Unsafe (dynamic identifiers):**

```scala
val userColumn = userInput  // If this comes from user, it's vulnerable!

// DANGEROUS - column name cannot be parameterized
userTable.filter(
  where(s"$userColumn = ?", value)
).runSyncAndGet

// If userColumn = "email; DROP TABLE users--", you have SQL injection!
```

### Safe Pattern for Dynamic Column Names

**Always validate against allowlist:**

```scala
def findByColumn(columnName: String, value: String): Seq[Row] = {
  // Define allowed columns
  val allowedColumns = Set("id", "name", "email", "status", "created_at")

  // Validate
  require(
    allowedColumns.contains(columnName),
    s"Invalid column: $columnName"
  )

  // Now safe to use
  userTable.filter(
    where(ps"$columnName = $value")  // Still use ps for value!
  ).runSyncAndGet
}

// Usage
findByColumn("email", "alice@example.com")  // ✓ Safe
findByColumn("password", "secret")          // ✗ Throws exception (not in allowlist)
findByColumn("id; DROP TABLE", "1")         // ✗ Throws exception
```

### Dangerous: executeSql

The `executeSql` method bypasses prepared statements entirely:

```scala
// ❌ DANGEROUS - never use with user input
db.executeSql(s"DELETE FROM users WHERE email = '$userInput'").runSyncAndGet

// ✅ Only use for trusted SQL (migrations, admin operations)
db.executeSql("""
  CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(256),
    email VARCHAR(256)
  )
""").runSyncAndGet
```

**When to use `executeSql`:**
- Database initialization (CREATE TABLE, CREATE INDEX)
- Schema migrations
- Admin operations with hardcoded SQL

**Never use `executeSql` with:**
- User input
- Dynamic queries
- Application logic

### Security Checklist

- [x] Use `ps""` interpolator for all dynamic values
- [x] Never interpolate user input into column/table names
- [x] Validate dynamic identifiers against allowlist
- [x] Avoid `executeSql` except for trusted SQL
- [x] Use prepared statements (automatic with `ps""`)
- [x] Review all WHERE clauses for safety

---

## Advanced Patterns

### map() - Transform Results

Extract specific columns without loading full rows:

```scala
// Extract just names
val names: Seq[String] = userTable.map(_.string("name")).runSyncAndGet

println(s"Users: ${names.mkString(", ")}")

// Extract multiple columns
case class UserSummary(id: Long, name: String, email: String)

val summaries: Seq[UserSummary] = userTable.map { row =>
  UserSummary(
    id = row.id,
    name = row.string("name"),
    email = row.string("email")
  )
}.runSyncAndGet
```

### Filtering in map()

Combine filtering and transformation:

```scala
// Get names of active users
val activeUserNames: Seq[String] = userTable.filter(
  where(ps"active = ${true}")
).map(_.string("name")).runSyncAndGet
```

### count() with Conditions

```scala
// Count active users
val activeCount = userTable.count(
  where(ps"active = ${true}")
).runSyncAndGet

// Count users in age range
val ageRangeCount = userTable.count(
  where(ps"age >= ${25} AND age <= ${40}")
).runSyncAndGet

println(s"Users aged 25-40: $ageRangeCount")
```

### Complex Filters with OR

sss.db doesn't have built-in OR support in the DSL, but you can write it directly:

```scala
// OR condition in WHERE clause
val users = userTable.filter(
  where("(active = true OR verified = true)")
).runSyncAndGet

// OR with parameters (careful with parentheses!)
val status1 = "active"
val status2 = "pending"

val users = userTable.filter(
  where(ps"(status = $status1 OR status = $status2)")
).runSyncAndGet
```

### Subqueries

For complex queries, use raw SQL with `executeSql` or join at the application level:

```scala
// Application-level join
val users = userTable.findAll().runSyncAndGet
val userIds = users.map(_.id)

val posts = postTable.filter(
  where("user_id").in(userIds)
).runSyncAndGet

val postsByUser = posts.groupBy(_.long("user_id"))

users.map { user =>
  val userPosts = postsByUser.getOrElse(user.id, Seq.empty)
  (user, userPosts)
}
```

### Custom Extractors

Create reusable extractors for complex types:

```scala
case class User(
  id: Long,
  name: String,
  email: String,
  age: Int,
  active: Boolean,
  createdAt: Long
)

object User {
  def fromRow(row: Row): User = User(
    id = row.id,
    name = row.string("name"),
    email = row.string("email"),
    age = row.int("age"),
    active = row.boolean("active"),
    createdAt = row.long("created_at")
  )
}

// Usage
val users: Seq[User] = userTable.map(User.fromRow).runSyncAndGet

users.foreach { user =>
  println(s"${user.name} (${user.age})")
}
```

---

## Performance Considerations

### Index Usage

Queries are only fast if the database can use indexes:

```scala
// ✅ Fast (if indexed on email)
userTable.filter(where(ps"email = $email")).runSyncAndGet

// ❌ Slow (LIKE with leading wildcard can't use index)
userTable.filter(where(ps"email LIKE ${s"%$domain"}")).runSyncAndGet

// ✅ Fast (LIKE with trailing wildcard can use index)
userTable.filter(where(ps"email LIKE ${s"$prefix%"}")).runSyncAndGet
```

**Tip:** Always WHERE on indexed columns for best performance.

### Avoid SELECT *

When you only need specific columns, create a custom View:

```scala
// ❌ Fetches all columns (including large blobs!)
val users = userTable.findAll().runSyncAndGet

// ✅ Fetch only needed columns
val userView = new View(
  tableName = "users",
  where = Where(),
  runContext = db.syncRunContext,
  freeBlobsEarly = false,
  cols = "id, name, email"  // Only these columns
)

val users = userView.findAll().runSyncAndGet  // Much faster!
```

### Use Pagination

Never load large result sets into memory at once:

```scala
// ❌ Bad for large tables
val allUsers = userTable.findAll().runSyncAndGet  // Could be millions of rows!

// ✅ Good - use pagination
val pagedView = userTable.toPaged(pageSize = 1000)
pagedView.toIterator.foreach { user =>
  processUser(user)
}
```

### N+1 Query Prevention

See [Performance Guide](performance.md#n1-query-prevention) for detailed patterns.

```scala
// ❌ N+1 queries (BAD!)
val users = userTable.findAll().runSyncAndGet
users.foreach { user =>
  val posts = postTable.filter(where(ps"user_id = ${user.id}")).runSyncAndGet
  // Creates one query per user!
}

// ✅ Batch query (GOOD!)
val users = userTable.findAll().runSyncAndGet
val userIds = users.map(_.id)
val posts = postTable.filter(where("user_id").in(userIds)).runSyncAndGet
// Only 2 queries total
```

---

## Summary

### Key Takeaways

**Basic Queries:**
- `find()`: Single result (Option[Row])
- `filter()`: Multiple results (Seq[Row])
- `findAll()`: All rows
- `count()`, `max()`: Aggregate operations

**WHERE Clauses:**
- Use `ps""` interpolator for safe parameterized queries
- Simple equality: `where("column" -> value)`
- Comparisons: `where(ps"age > ${25}")`
- LIKE patterns: `where(ps"name LIKE ${"%alice%"}")`

**Method Chaining:**
- `.and()`: Add conditions
- `.orderBy()`: Sort results
- `.limit()`, `.offset()`: Pagination
- `.in()`, `.notIn()`: Match collections

**Security:**
- Always use `ps""` for values
- Never interpolate user input into column/table names
- Validate dynamic identifiers against allowlist
- Avoid `executeSql` with user input

**Performance:**
- Use indexes on WHERE columns
- Avoid `SELECT *` (create Views with specific columns)
- Use pagination for large result sets
- Prevent N+1 queries with batching

### Next Steps

- **[CRUD Operations](crud-operations.md)** - Complete guide to insert, update, delete
- **[Transactions](transactions.md)** - Compose queries in transactions
- **[Performance](performance.md)** - Optimize query performance
- **[Troubleshooting](troubleshooting.md)** - Common query issues
