package sss.db

import java.io._
import java.nio.file.Files
import java.util
import org.scalatest.DoNotDiscover

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.util.Random


object BlobStoreHelper {


  val KB = 1024

  val rootFolder = Files.createTempDirectory("").toFile // new File("/extra/home/alan")

  lazy val elem = {
    val e = new Array[Byte](KB)
    Random.nextBytes(e)
    e
  }

  def toStream(sizeInKb: Int): LazyList[Array[Byte]] = LazyList.tabulate(sizeInKb)(_ => elem)

  def createFile(sizeInKb: Int): File = {

    val f = new File(rootFolder, Random.nextLong().toString)
    writeBytes(toStream(sizeInKb), f)
    f
  }


  def writeBytes(data: LazyList[Array[Byte]], file: File) = {
    val target = new BufferedOutputStream(new FileOutputStream(file))
    try data.foreach(target.write) finally target.close
  }

}
/**
  * Created by alan on Feb 19.
  */
@DoNotDiscover
class BlobStoreSpec extends DbSpecSetup {


  import BlobStoreHelper._

  it should "support writing File to BLOB" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val start = System.currentTimeMillis()

    val numB = KB * 10
    val table = fixture.dbUnderTest.table("testBinary")
    val fIn = createFile(numB)
    fIn.deleteOnExit()

    val startWrite = System.currentTimeMillis()
    println(s"Setup/Create ${numB} KB file took ${startWrite-start} ms")

    val in = new FileInputStream(fIn)
    val index = table.insert(Map("blobVal" -> in)).runSyncAndGet

    val writeDone = System.currentTimeMillis()
    println(s"Write ${numB} KB file to db took ${writeDone - startWrite} ms")

    val found = table.get(index.id).runSyncAndGet
    assert(found.isDefined)
    val is = found.get[InputStream]("blobVal")

    val fOut = new File(rootFolder, s"${fIn.getName}.out")
    fOut.deleteOnExit()

    val os = new BufferedOutputStream(new FileOutputStream(fOut))
    val bytes = new Array[Byte](KB)

    try {
      LazyList
        .continually(is.read(bytes))
        .takeWhile(_ != -1)
        .foreach(read => os.write(bytes, 0, read))
    } finally {
      os.close()
    }

    val readDone = System.currentTimeMillis()
    println(s"Read ${numB} KBfile from db took ${readDone - writeDone} ms")

    val f1 = new FileInputStream(fIn)
    val f2 = new FileInputStream(fOut)
    val bytes1 = new Array[Byte](KB)
    val bytes2 = new Array[Byte](KB)

    LazyList
      .continually(f1.read(bytes1), f1.read(bytes2))
      .takeWhile(rr => rr._1 != -1 && rr._2 != -1)
      .foreach(_ => assert(util.Arrays.equals(bytes1, bytes2), "Read arrays did not match"))

    val compareDone = System.currentTimeMillis()
    println(s"Compare 2 ${numB} KB files took ${compareDone - writeDone} ms")
  }

  it should " support persisting a byte as Binary" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val table = db.table("testBinary")

    val testByte: Byte = 34

    (for {

      m <- table.persist(Map("byteVal" -> testByte))
      _ = assert(m[Byte]("byteVal") === testByte)
      empty <- table.persist(Map("byteVal" -> None))
      _ = assert(Option(empty[Byte]("byteVal")).isEmpty)
      _ = assert(empty[Option[Byte]]("byteVal") === None)

    } yield ()).runSyncAndGet

  }

  it should " support persisting binary arrays as a blob " in {

    val testStr = "Hello My Friend"

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val table = db.table("testBinary")

    val plan = for {
      m <- table.persist(Map("blobVal" -> testStr.getBytes))
      _ = assert(m[Array[Byte]]("blobVal") === testStr.getBytes)
      _ = assert(new String(m[Array[Byte]]("blobVal")) === testStr)
    } yield()

    plan.runSyncAndGet
  }

  it should "support persisting wrapped binary arrays as a blob" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val table = db.table("testBinary")

    val testStr = "Hello My Friend"

    val wAry : Array[Byte] = testStr.getBytes

    val plan = for {
      m <- table.persist(Map("blobVal" -> wAry))
      _ = assert(m[Array[Byte]]("blobVal") === wAry)
      _ = assert(new String(m[Array[Byte]]("blobVal").array) === testStr)
    } yield ()

    plan.runSyncAndGet

  }

  it should "support find along binary arrays" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val table = db.table("testBinary")
    val testStr = "Hello My Friend"
    val bytes = testStr.getBytes

    val plan = for {
      _ <- table.persist(Map("blobVal" -> bytes))
      found <- table.find(where("blobVal = ?", bytes))
      _ = assert(found.isDefined)
      _ = assert(found.get[Array[Byte]]("blobVal") === bytes)
      _ = assert(new String(found.get[mutable.ArraySeq[Byte]]("blobVal").toArray, StandardCharsets.UTF_8) === testStr)
    } yield ()

    plan.runSyncAndGet

  }

  it should " NOT support find along wrapped binary arrays (you must use .array)" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    val table = db.table("testBinary")

    val testStr = "Hello My Friend"

    val wAry : Array[Byte] = testStr.getBytes

    val plan = for {
      m <- table.persist(Map("blobVal" -> wAry))
      found <- table.find(where("blobVal = ?", wAry.array))
      _ = assert(found.isDefined)
      _ =  assert(found.get[Array[Byte]]("blobVal") === wAry)
      _ = assert(new String(found.get[Array[Byte]]("blobVal").array) === testStr)
    } yield ()

    plan.runSyncAndGet

  }

  it should "support inputstream result" in {

    val db = fixture.dbUnderTest
    import db.syncRunContext.ds
    import db.syncRunContext
    val table = db.table("testBinary")
    val testStr = "Hello My Friend"
    val bytes = testStr.getBytes


    val plan = for {
      _ <- table.persist(Map("blobVal" -> bytes))
      found <- table.find(where("blobVal = ?", bytes))
      _ = assert(found.isDefined)
      is = found.get[InputStream]("blobVal")
      readBytes = new Array[Byte](bytes.length)
      dataIs = new DataInputStream(is)
      _ = dataIs.readFully(readBytes)
      _ = assert(readBytes === bytes)
      _ = assert(new String(readBytes) === testStr)
    } yield ()

    plan.runSyncAndGet
  }



  /*it should "support blob result" in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val bytes = testStr.getBytes
    table.persist(Map("blobVal" -> bytes))

    val found = table.find(where("blobVal"->  bytes))
    assert(found.isDefined)
    val blob = found.get.blob("blobVal")
    val readBytes = blob.getBytes(1, bytes.length)
    blob.free()

    assert(readBytes === bytes)
    assert(new String(readBytes) === testStr)
  }*/

}
