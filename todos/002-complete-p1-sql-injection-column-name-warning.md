---
status: complete
priority: p1
issue_id: 002
tags: [code-review, security, documentation]
dependencies: []
---

# Problem Statement

CLAUDE.md documents prepared statements (lines 67-70) but fails to warn about SQL injection risks with dynamic column names, table names, and SQL keywords. The codebase contains vulnerable patterns (PagedView.scala uses string interpolation for column names) that could be exploited if user input is not validated.

**Why it matters:** SQL injection is OWASP Top 10. Users may incorrectly assume the `ps` interpolator protects all SQL fragments, leading to critical security vulnerabilities in production.

## Findings

**Security Sentinel:**
- Identified string interpolation for column names in PagedView.scala:51,57
- Found validation exists for OrderBy but not consistently applied
- No documentation of when to use `s"$var"` vs `ps"$var"`

**Code Evidence:**
```scala
// From PagedView.scala - VULNERABLE if indexCol is user-controlled
where(s"$indexCol > ?", lastIndexInPage)
```

**Attack Vector:**
```scala
val userColumn = "id; DROP TABLE users--" // Malicious input
where(s"$userColumn = ?", value) // SQL injection!
```

**Mitigation Found (package.scala:173-176):**
```scala
val regex = "^[a-zA-Z_][a-zA-Z0-9_]*$"
require(pattern.matcher(colName).matches(), s"Column name must conform to pattern $regex")
```
But this is only applied to OrderBy, not all column name usage.

## Proposed Solutions

### Option 1: Add Security Section with SQL Injection Warnings (Recommended)
**Implementation:**
Add new section after Configuration:

```markdown
## Security Considerations

### SQL Injection Prevention

This library uses prepared statements for **values**, which prevents injection. However, table names, column names, and SQL keywords **cannot be parameterized**.

**Safe (parameterized values):**
```scala
where(ps"email = $userInput AND status = $statusInput")  // ✓ SAFE
```

**Unsafe (dynamic identifiers):**
```scala
where(s"$userColumn = ?", value)  // ✗ VULNERABLE if userColumn is untrusted
table.select(s"SELECT * FROM $userTable") // ✗ VULNERABLE
```

**For dynamic column names, use validation:**
```scala
val allowedColumns = Set("id", "email", "status", "created_at")
require(allowedColumns.contains(columnName), s"Invalid column: $columnName")
where(s"$columnName = ?", value) // Now safe
```

**Validation regex for identifiers:**
```scala
val validIdentifier = "^[a-zA-Z_][a-zA-Z0-9_]*$"
require(columnName.matches(validIdentifier), "Invalid identifier")
```

### Dangerous Operations

The `executeSql` method bypasses prepared statements:
```scala
db.executeSql(rawSql)  // ⚠️ SQL injection risk if rawSql contains user input
```

**Only use for:**
- Trusted SQL (migrations, admin operations)
- DDL statements (CREATE TABLE, ALTER TABLE)

**Never use for:**
- User input
- Dynamic queries based on request parameters
```

**Pros:**
- Comprehensive security guidance
- Shows both anti-patterns and solutions
- Documents validation patterns used in codebase

**Cons:**
- Adds ~30 lines to documentation

**Effort:** Small (15 minutes)
**Risk:** None - purely additive documentation

### Option 2: Apply Column Name Validation Throughout Codebase
**Implementation:** Add regex validation to all column name usage, not just OrderBy.

**Pros:**
- Prevents injection at code level
- Defense in depth

**Cons:**
- Breaking change if users have non-standard column names
- Requires extensive testing
- Doesn't eliminate need for documentation

**Effort:** Large (4+ hours)
**Risk:** High - potential breaking changes

### Option 3: Add Inline Warning to Prepared Statement Section
**Implementation:** Add brief warning at lines 67-70.

**Pros:**
- Quick fix
- Contextual to existing content

**Cons:**
- Incomplete coverage
- Doesn't address other security concerns

**Effort:** Small (5 minutes)
**Risk:** Low but insufficient

## Recommended Action

**Option 1** - Add comprehensive Security Considerations section.

This provides complete security guidance, documents dangerous operations, and shows validation patterns without requiring code changes.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (add Security Considerations section)

**Components:**
- Documentation

**Database Changes:** None

**Vulnerable Patterns in Codebase:**
- PagedView.scala:51,57 - String interpolation for column names
- UpdatableView.scala - `update(String, String)` marked dangerous in comments

**Existing Protections:**
- OrderBy column validation (package.scala:173-176)
- executeSql warning in code comments (Db.scala:98)
- ps interpolator for values (package.scala:141-145)

## Acceptance Criteria

- [ ] Security Considerations section exists in CLAUDE.md
- [ ] Documents when ps vs s interpolation is appropriate
- [ ] Provides validation regex for column names
- [ ] Lists dangerous operations (executeSql, update(String, String))
- [ ] Shows both anti-patterns and secure alternatives
- [ ] Warns against SQL injection with dynamic identifiers

## Work Log

**2026-01-28:** Issue identified during security review. Security-sentinel agent found vulnerable patterns in PagedView.scala and missing documentation warnings.

**2026-01-28:** ✅ FIXED - Added comprehensive Security Considerations section to CLAUDE.md after Important Patterns. Includes SQL injection warnings, safe/unsafe patterns, validation examples, and dangerous operations warning.

## Resources

- **OWASP SQL Injection:** https://owasp.org/www-community/attacks/SQL_Injection
- **Related Files:**
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/PagedView.scala` (vulnerable pattern)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/package.scala` (validation regex)
  - `/home/alan/develop/sss.db/src/main/scala/sss/db/Db.scala` (executeSql warnings)
