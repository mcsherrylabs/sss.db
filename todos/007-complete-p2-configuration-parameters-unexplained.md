---
status: complete
priority: p2
issue_id: 007
tags: [code-review, documentation, configuration]
dependencies: []
---

# Problem Statement

CLAUDE.md Configuration section (lines 86-103) shows configuration keys without explaining their purpose, valid values, or when to adjust them. Users don't understand what `viewCachesSize`, `freeBlobsEarly`, or `useShutdownHook` do, leading to suboptimal configuration.

**Why it matters:** Improper configuration causes performance issues (cache too small), memory leaks (shutdown hook disabled without manual cleanup), and incorrect behavior (blob memory retention).

## Findings

**Pattern Recognition Specialist & Performance Oracle:**
- `viewCachesSize = 100` shown with no explanation
- `freeBlobsEarly = false` not explained (impacts memory)
- `useShutdownHook = false` purpose unclear
- HikariCP properties shown in test config but not documented
- Transaction isolation levels missing

**Actual Usage (from codebase):**
- `viewCachesSize`: Cache size for View metadata (Query.scala:38-49)
- `freeBlobsEarly`: Release blob memory immediately after extraction
- `useShutdownHook`: Register JVM shutdown hook to close connections
- HikariCP: 17 properties in test config, only 5 shown in docs

## Proposed Solutions

### Option 1: Add Configuration Reference Subsection (Recommended)
**Implementation:**
Expand Configuration section with detailed explanations:

```markdown
## Configuration

Database configuration goes in `application.conf` (or test resources):

### Core Settings

```
database {
  datasource {
    driver = "org.hsqldb.jdbc.JDBCDriver"
    connection = "jdbc:hsqldb:mem:test"
    user = "SA"
    pass = ""
    maxPoolSize = 10
  }

  # View metadata cache size (default: 100)
  # Caches column metadata per View to avoid repeated DB calls
  # Increase for apps with many distinct views (>100 unique queries)
  # Each cache entry is small (~1KB), safe to increase to 1000+
  viewCachesSize = 100

  # Register JVM shutdown hook to close connections (default: true)
  # Set false if managing lifecycle manually or using container shutdown hooks
  useShutdownHook = false

  # Release blob memory immediately after extraction (default: false)
  # Set true to free memory sooner at cost of small performance overhead
  # Recommended for large blobs or high memory pressure
  freeBlobsEarly = false

  # Optional: SQL to run on database startup
  deleteSql = ["DROP TABLE IF EXISTS old_table"]
  createSql = ["CREATE TABLE IF NOT EXISTS my_table (id BIGINT PRIMARY KEY, ...)"]
}
```

### Transaction Isolation Levels

Configure per-database instance:
```scala
import sss.db.TxIsolationLevel._

Db(config, dataSource, executionContext, READ_COMMITTED)
```

Available levels:
- `READ_UNCOMMITTED`: Allows dirty reads (fastest, least safe)
- `READ_COMMITTED`: Default, prevents dirty reads
- `REPEATABLE_READ`: Prevents non-repeatable reads
- `SERIALIZABLE`: Strictest isolation (slowest, safest)

### Connection Pool Tuning (HikariCP)

Advanced HikariCP properties:
```
datasource {
  # Connection pool sizing
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
```

**Pros:**
- Complete configuration reference
- Explains purpose and impact of each setting
- Provides sizing guidance
- Documents HikariCP tuning

**Cons:**
- Adds ~70 lines to Configuration section

**Effort:** Medium (35 minutes)
**Risk:** None - purely additive documentation

### Option 2: Add Inline Comments to Configuration Example
**Implementation:** Brief comments next to each key.

**Pros:**
- Minimal growth
- Contextual

**Cons:**
- Space constraints
- Limited detail

**Effort:** Small (10 minutes)
**Risk:** Low but incomplete

### Option 3: Link to Configuration Reference
**Implementation:** Reference external config documentation.

**Pros:**
- Keeps CLAUDE.md concise
- Detailed coverage possible

**Cons:**
- Discoverability
- External dependency

**Effort:** Small (5 minutes)
**Risk:** Medium - users won't follow link

## Recommended Action

**Option 1** - Add comprehensive Configuration Reference subsection.

This provides complete configuration understanding where users need it.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (expand Configuration section)

**Components:**
- Documentation

**Database Changes:** None

**Configuration Parameters:**
```scala
// DbConfig.scala
case class DbConfig(
  dataSource: DataSourceConfig,
  viewCachesSize: Int = 100,
  useShutdownHook: Boolean = true,
  freeBlobsEarly: Boolean = false,
  deleteSqlOpt: Option[Seq[String]] = None,
  createSqlOpt: Option[Seq[String]] = None,
  transactionIsolationLevel: TxIsolationLevel = TxIsolationLevel.READ_COMMITTED
)
```

## Acceptance Criteria

- [ ] Each configuration key has purpose explanation
- [ ] Sizing guidance provided for viewCachesSize and maxPoolSize
- [ ] freeBlobsEarly impact on memory documented
- [ ] useShutdownHook implications explained
- [ ] Transaction isolation levels documented
- [ ] HikariCP advanced properties reference added
- [ ] Connection pool sizing guidelines included
- [ ] Default values stated for all parameters

## Work Log

**2026-01-28:** Issue identified by performance-oracle and pattern-recognition-specialist agents. Found extensive HikariCP configuration in test resources completely undocumented.

## Resources

- **HikariCP Configuration:** https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby
- **Related Files:**
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/Db.scala` (DbConfig case class)
  - `/home/alan/develop/sss.db/src/test/resources/application.conf` (full HikariCP example)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/Query.scala` (viewCachesSize usage)
