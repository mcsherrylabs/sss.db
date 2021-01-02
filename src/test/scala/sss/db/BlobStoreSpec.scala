package sss.db

import java.io._
import java.nio.file.Files
import java.util
import org.scalatest.DoNotDiscover

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.Random


/**
  * Created by alan on Feb 19.
  */
@DoNotDiscover
class BlobStoreSpec extends DbSpecSetup {


  def writeBytes(data: Stream[Array[Byte]], file: File) = {
    val target = new BufferedOutputStream(new FileOutputStream(file))
    try data.foreach(target.write) finally target.close
  }

  val KB = 1024

  lazy val elem = {
    val e = new Array[Byte](KB)
    Random.nextBytes(e)
    e
  }

  val rootFolder = new File("/extra/home/alan") //Files.createTempDirectory("").toFile //

  def toStream(sizeInKb: Int): Stream[Array[Byte]] = Stream.tabulate(sizeInKb)(_ => elem)

  def createFile(sizeInKb: Int): File = {

    val f = new File(rootFolder, Random.nextLong().toString)
    writeBytes(toStream(sizeInKb), f)
    f
  }

  it should "support writing File to BLOB" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val start = System.currentTimeMillis()

    val numKB = KB * 10
    val table = fixture.dbUnderTest.table("testBinary")
    val fIn = createFile(numKB)
    fIn.deleteOnExit()

    val startWrite = System.currentTimeMillis()
    println(s"Setup/Create ${numKB} KB file took ${startWrite - start} ms")

    val in = new FileInputStream(fIn)
    val index = table.insert(Map("blobVal" -> in)).runSyncUnSafe

    val writeDone = System.currentTimeMillis()
    println(s"Write ${numKB} KB file to db took ${writeDone - startWrite} ms")

    val found = table.get(index.id).runSyncUnSafe
    assert(found.isDefined)
    val is = found.get[InputStream]("blobVal")

    val fOut = new File(rootFolder, s"${fIn.getName}.out")
    fOut.deleteOnExit()

    val os = new BufferedOutputStream(new FileOutputStream(fOut))
    val bytes = new Array[Byte](KB)

    try {
      Stream
        .continually(is.read(bytes))
        .takeWhile(_ != -1)
        .foreach(read => os.write(bytes, 0, read))
    } finally {
      os.close()
    }

    val readDone = System.currentTimeMillis()
    println(s"Read ${numKB} KBfile from db took ${readDone - writeDone} ms")

    val f1 = new FileInputStream(fIn)
    val f2 = new FileInputStream(fOut)
    val bytes1 = new Array[Byte](KB)
    val bytes2 = new Array[Byte](KB)

    Stream
      .continually(f1.read(bytes1), f1.read(bytes2))
      .takeWhile(rr => rr._1 != -1 && rr._2 != -1)
      .foreach(_ => assert(util.Arrays.equals(bytes1, bytes2), "Read arrays did not match"))

    val compareDone = System.currentTimeMillis()
    println(s"Compare 2 ${numKB} KB files took ${compareDone - writeDone} ms")
  }

  it should " support persisting a byte as Binary" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testBinary")

    val testByte: Byte = 34

    (for {

      m <- table.persist(Map("byteVal" -> testByte))
      _ = assert(m[Byte]("byteVal") === testByte)
      empty <- table.persist(Map("byteVal" -> None))
      _ = assert(Option(empty[Byte]("byteVal")).isEmpty)
      _ = assert(empty[Option[Byte]]("byteVal") === None)

    } yield ()).runSyncUnSafe

  }

  it should " support persisting binary arrays as a blob " in {

    val testStr = "Hello My Friend"

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testBinary")

    val plan = for {
      m <- table.persist(Map("blobVal" -> testStr.getBytes))
      _ = assert(m[Array[Byte]]("blobVal") === testStr.getBytes)
      _ = assert(new String(m[Array[Byte]]("blobVal")) === testStr)
    } yield()

    plan.runSyncUnSafe
  }

  it should "support persisting wrapped binary arrays as a blob" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testBinary")

    val testStr = "Hello My Friend"

    val wAry : Array[Byte] = testStr.getBytes

    val plan = for {
      m <- table.persist(Map("blobVal" -> wAry))
      _ = assert(m[Array[Byte]]("blobVal") === wAry)
      _ = assert(new String(m[Array[Byte]]("blobVal").array) === testStr)
    } yield ()

    plan.runSyncUnSafe

  }

  it should "support find along binary arrays" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
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

    plan.runSyncUnSafe

  }

  it should " NOT support find along wrapped binary arrays (you must use .array)" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
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

    plan.runSyncUnSafe

  }

  it should " support inputstream result " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
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

    plan.runSyncUnSafe
  }

}

