---
status: complete
priority: p1
issue_id: 003
tags: [code-review, security, documentation, credentials]
dependencies: []
---

# Problem Statement

CLAUDE.md configuration section (lines 83-104) shows database credentials in plaintext without security warnings, best practices for credential management, or guidance on using environment variables. This could lead developers to commit credentials to version control or use insecure configuration practices.

**Why it matters:** Leaked credentials in version control are a critical security incident. Documentation should actively prevent this by showing secure patterns and explicit warnings.

## Findings

**Security Sentinel:**
- Configuration example shows plaintext password (empty string)
- No warning against committing credentials
- No guidance on environment variable substitution
- No mention of .gitignore for config files

**Current Documentation (Lines 87-93):**
```
database {
  datasource {
    driver = "org.hsqldb.jdbc.JDBCDriver"
    connection = "jdbc:hsqldb:mem:test"
    user = "SA"
    pass = ""  # ← Plaintext password, no warning
    maxPoolSize = 10
  }
}
```

**Evidence of Secure Practices Elsewhere:**
- build.sbt uses environment variables: `SONA_USER`, `SONA_PASS`
- Project already understands secure credential management
- Not documented in CLAUDE.md

## Proposed Solutions

### Option 1: Add Explicit Credential Security Warning (Recommended)
**Implementation:**
Replace configuration section with secure example and warnings:

```markdown
## Configuration

Database configuration goes in `application.conf` (or test resources):

⚠️ **SECURITY WARNING:** Never commit database credentials to version control. Use environment variables or external secret management.

### Production Configuration
```
database {
  datasource {
    driver = "org.postgresql.Driver"
    connection = ${DATABASE_URL}  # From environment variable
    user = ${DATABASE_USER}       # From environment variable
    pass = ${DATABASE_PASSWORD}   # From environment variable
    maxPoolSize = 10
  }

  viewCachesSize = 100
  useShutdownHook = false
  freeBlobsEarly = false
}
```

**Environment variable substitution:**
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
}
```

**Best Practices:**
- Add `application.conf` to `.gitignore`
- Use `application.conf.example` as template with placeholder values
- Store production credentials in secret management (AWS Secrets Manager, HashiCorp Vault, etc.)
- Never log connection strings containing credentials
```

**Pros:**
- Prominent security warning
- Shows both production and test patterns
- Documents HOCON environment variable syntax
- Provides actionable guidance

**Cons:**
- Longer configuration section

**Effort:** Small (10 minutes)
**Risk:** None - purely documentation improvement

### Option 2: Move Credentials to External File Pattern
**Implementation:** Document separate credentials.conf pattern.

**Pros:**
- Clean separation of concerns

**Cons:**
- More complex setup
- Not standard HOCON practice

**Effort:** Medium (20 minutes)
**Risk:** Low - adds complexity

### Option 3: Reference External Security Guide
**Implementation:** Link to external credential management guide.

**Pros:**
- Minimal documentation changes

**Cons:**
- Users may not follow external link
- Doesn't show library-specific patterns

**Effort:** Small (2 minutes)
**Risk:** Medium - incomplete guidance

## Recommended Action

**Option 1** - Add explicit security warning and show environment variable substitution.

This provides complete, actionable guidance without requiring users to consult external resources.

## Technical Details

**Affected Files:**
- `/home/alan/develop/sss.db/CLAUDE.md` (lines 83-104)

**Components:**
- Documentation
- Configuration examples

**Database Changes:** None

**HOCON Syntax:**
- `${VAR}` - Substitutes environment variable (required)
- `${?VAR}` - Substitutes environment variable (optional)
- Documented at: https://github.com/lightbend/config/blob/main/HOCON.md

## Acceptance Criteria

- [ ] Prominent security warning about credential management
- [ ] Production configuration example uses environment variables
- [ ] Test configuration example clearly marked as test-only
- [ ] Documents HOCON environment variable syntax (${VAR})
- [ ] Mentions .gitignore for application.conf
- [ ] Provides guidance on secret management systems
- [ ] Warns against logging credentials

## Work Log

**2026-01-28:** Issue identified during security review. Security-sentinel agent noted plaintext credentials without warnings. Found that project uses secure practices (env vars in build.sbt) but not documented in CLAUDE.md.

## Resources

- **OWASP Secrets Management:** https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- **HOCON Specification:** https://github.com/lightbend/config/blob/main/HOCON.md
- **Related Files:**
  - `/home/alan/develop/sss.db/build.sbt` (uses SONA_USER, SONA_PASS from env)
  - `/home/alan/develop/sss.db/src/test/resources/application.conf` (test config)
