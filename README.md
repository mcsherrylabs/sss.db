# sss.db
Scala module for basic sql db access.

[![Build Status](https://travis-ci.org/mcsherrylabs/sss.db.svg?branch=master)](https://travis-ci.org/mcsherrylabs/sss.db) [![Coverage Status](https://coveralls.io/repos/github/mcsherrylabs/sss.db/badge.svg?branch=master)](https://coveralls.io/github/mcsherrylabs/sss.db?branch=master)

A simple way to deal with reading and inserting into sql tables. 

Supports 
 - transactions 
 - optimistic versioning
 - connection pool
 - insert, update, delete, find, filter, count, page, map, order ...
 - uses prepared statements to aid SQL injection prevention

In build.sbt add ...
```
resolvers += "stepsoft" at "http://nexus.mcsherrylabs.com/repository/releases/"
 
libraryDependencies += "mcsherrylabs.com" %% "sss-db" % "0.9.36"
```

Define a database in application.conf
```
database {   
  datasource {
    driver = "org.hsqldb.jdbc.JDBCDriver"
    connection = "jdbc:hsqldb:test"
    user = "SA"
    pass = ""	
    deleteSql = ["DROP VIEW testview", "DROP TABLE test", "DROP table testVersion" ]
    createSql = ["CREATE TABLE IF NOT EXISTS testTable (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1), strId VARCHAR(256), createTime BIGINT, intVal INTEGER)",
		"CREATE VIEW testview AS SELECT strId, intVal FROM test WHERE intVal > 50",
		"CREATE TABLE IF NOT EXISTS testVersion (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1), strId VARCHAR(256), version BIGINT)"]
  }
}
```
(Remove the delete and/or create sql if you don't want them to run. If the table definition contains a 'version' column, optimistic locking is supported automagically)

Create an instance of a Db (picks up 'database' config by default) and work with a table...

```
 val db = Db()
 val myTable = db.testTable
 // persist a row 
 val row = myTable.persist(Map("strId" -> "Hello World", "createTime" -> new Date, "intVal" -> 45))
 // retrieve row by id 
 val id  = row("id")
 val sameRow = myTable(id)
```  
Make several updates in a single transaction ... ('tx' is an alias for 'inTransaction')

```
 import myTable._
 
 inTransaction {
    val row = myTable.persist(Map("strId" -> "Hello World", "createTime" -> new Date, "intVal" -> 45))
    myOtherTable.persist(Map("myTableId" -> row("id"), "payload" -> "associated info 1")
    myOtherTable.persist(Map("myTableId" -> row("id"), "payload" -> "associated info 2")
 }
 ```

Note that when working with Byte Arrays, extracting to a byte array must happen *inside* the transaction. Otherwise the 
blob is not guaranteed to be available.  
 ```
 table.tx {
       val found = table.find(where(ps"blobVal = $bytes")) // creates parameterised prepared statements
       assert(found.isDefined)
       assert(found.get[Array[Byte]]("blobVal") === bytes) //<- apply[Array[Byte]]("blobVal") INSIDE tx....
     }
 ```

Write natural looking access code ...
```
val rows = view.filter(
      where (filter)
      orderBy OrderDesc(indexCol)
      limit pageSize)
```

Use for comprehensions and sql queries together ...
```
val allDomainObjs = for {
      name <- tableNames // salary tables are named after each month!
      table = db.table(name)
      rows <- table.toPaged(300)
      row  <- rows
      if(row[Long](salaryCol) > 100000)
    } yield (Domain(row))
```    
