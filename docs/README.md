# sss.db Documentation

Welcome to the sss.db documentation! This guide will help you find exactly what you need.

## Getting Started

New to sss.db? Start here:

- **[Getting Started Guide](getting-started.md)** - 30-minute tutorial from zero to first application
  - Installation and setup
  - First database connection
  - CRUD operations
  - Transaction composition
  - Complete working examples

- **[Core Concepts](core-concepts.md)** - Understand the architectural abstractions
  - FutureTx monad
  - RunContext (Sync vs Async)
  - View hierarchy
  - Row and type-safe accessors
  - Where query DSL

## Guides

### Database Operations

- **[Transactions](transactions.md)** - Compose operations safely
  - ACID guarantees
  - Error handling patterns
  - Optimistic locking
  - Transaction isolation levels
  - Retry strategies

- **[Queries](queries.md)** - Query DSL and filtering
  - WHERE clauses with prepared statements
  - IN/NOT IN clauses
  - Ordering and pagination
  - SQL injection prevention
  - Advanced filtering patterns

- **[CRUD Operations](crud-operations.md)** - Create, Read, Update, Delete
  - Insert vs persist patterns
  - Update strategies
  - Delete operations
  - Blob handling
  - Batch operations

### Configuration and Deployment

- **[Configuration](configuration.md)** - Database setup and tuning
  - Connection pool configuration
  - Security (environment variables)
  - HikariCP tuning
  - Multiple databases
  - Production settings

- **[Performance](performance.md)** - Optimization patterns
  - N+1 query prevention
  - Batch operations
  - Large result set handling
  - Connection pool sizing
  - Concurrency patterns

### Development and Testing

- **[Testing](testing.md)** - Test your code
  - Test setup with HSQLDB
  - Test fixtures and patterns
  - Testing FutureTx operations
  - Example test patterns

- **[Troubleshooting](troubleshooting.md)** - Solve common issues
  - Connection pool exhausted
  - Slow queries
  - Optimistic locking failures
  - Memory leaks
  - Transaction deadlocks
  - Configuration errors

## Reference

- **[API Documentation](api/scaladoc/index.html)** - Complete ScalaDoc reference
  - Browse by package
  - Search by type or method
  - View source code

## Quick Links

### By Task

**I want to...**
- Get started → [Getting Started](getting-started.md)
- Understand architecture → [Core Concepts](core-concepts.md)
- Write safe transactions → [Transactions](transactions.md)
- Query data → [Queries](queries.md)
- Insert/update/delete → [CRUD Operations](crud-operations.md)
- Configure database → [Configuration](configuration.md)
- Optimize performance → [Performance](performance.md)
- Write tests → [Testing](testing.md)
- Fix an issue → [Troubleshooting](troubleshooting.md)

### By Experience Level

**Beginner:**
1. [Getting Started](getting-started.md) - First steps
2. [Core Concepts](core-concepts.md) - Understanding basics
3. [CRUD Operations](crud-operations.md) - Basic operations

**Intermediate:**
1. [Transactions](transactions.md) - Composing operations
2. [Queries](queries.md) - Advanced filtering
3. [Configuration](configuration.md) - Production setup

**Advanced:**
1. [Performance](performance.md) - Optimization
2. [Troubleshooting](troubleshooting.md) - Problem solving
3. [API Documentation](api/scaladoc/index.html) - Deep dive

## Additional Resources

- **[GitHub Repository](https://github.com/mcsherrylabs/sss.db)** - Source code and examples
- **[Issue Tracker](https://github.com/mcsherrylabs/sss.db/issues)** - Report bugs or request features
- **[Maven Central](https://search.maven.org/artifact/com.mcsherrylabs/sss-db_2.13)** - Latest releases

## Contributing

Found an error in the documentation? Want to add examples?

1. Fork the repository
2. Edit the documentation in `docs/`
3. Submit a pull request

All contributions welcome!

---

**Documentation Version:** 0.9.57  
**Last Updated:** January 2026
