package sss.db

import java.io._
import java.nio.file.Files
import java.util

import org.scalatest.DoNotDiscover

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

  def toStream(sizeInKb: Int): Stream[Array[Byte]] = Stream.tabulate(sizeInKb)(_ => elem)

  def createFile(sizeInKb: Int): File = {

    val f = new File(rootFolder, Random.nextLong().toString)
    writeBytes(toStream(sizeInKb), f)
    f
  }


  def writeBytes(data: Stream[Array[Byte]], file: File) = {
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

    val start = System.currentTimeMillis()

    val numB = KB * 10
    val table = fixture.dbUnderTest.testBinary
    val fIn = createFile(numB)
    fIn.deleteOnExit()

    val startWrite = System.currentTimeMillis()
    println(s"Setup/Create ${numB} KB file took ${startWrite-start} ms")

    val in = new FileInputStream(fIn)
    val index = table.insert(Map("blobVal" -> in))

    val writeDone = System.currentTimeMillis()
    println(s"Write ${numB} KB file to db took ${writeDone - startWrite} ms")

    val found = table.get(index.id)
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
    println(s"Read ${numB} KBfile from db took ${readDone - writeDone} ms")

    val f1 = new FileInputStream(fIn)
    val f2 = new FileInputStream(fOut)
    val bytes1 = new Array[Byte](KB)
    val bytes2 = new Array[Byte](KB)

    Stream
      .continually(f1.read(bytes1), f1.read(bytes2))
      .takeWhile(rr => rr._1 != -1 && rr._2 != -1)
      .foreach(_ => assert(util.Arrays.equals(bytes1, bytes2), "Read arrays did not match"))

    val compareDone = System.currentTimeMillis()
    println(s"Compare 2 ${numB} KB files took ${compareDone-writeDone} ms")
  }

  it should " support persisting a byte as Binary" in {

    val testByte: Byte = 34
    val table = fixture.dbUnderTest.testBinary
    table.tx {
      val m = table.persist(Map("byteVal" -> testByte))
      assert(m[Byte]("byteVal") === testByte)
      val empty = table.persist(Map("byteVal" -> None))
      assert(Option(empty[Byte]("byteVal")).isEmpty)
      assert(empty[Option[Byte]]("byteVal") === None)
    }

  }

  it should " support persisting binary arrays as a blob " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    table.tx {
      val m = table.persist(Map("blobVal" -> testStr.getBytes))
      assert(m[Array[Byte]]("blobVal") === testStr.getBytes)
      assert(new String(m[Array[Byte]]("blobVal")) === testStr)
    }

  }

  it should "support persisting wrapped binary arrays as a blob" in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val wAry : mutable.WrappedArray[Byte] = testStr.getBytes
    table.tx {
      val m = table.persist(Map("blobVal" -> wAry))
      assert(m[mutable.WrappedArray[Byte]]("blobVal") === wAry)
      assert(new String(m[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }
  it should "support find along binary arrays" in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val bytes = testStr.getBytes
    table.persist(Map("blobVal" -> bytes))

    table.tx {
      val found = table.find(where("blobVal = ?", bytes))
      assert(found.isDefined)
      assert(found.get[Array[Byte]]("blobVal") === bytes)
      assert(new String(found.get[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }

  it should " NOT support find along wrapped binary arrays (you must use .array)" in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val wAry : mutable.WrappedArray[Byte] = testStr.getBytes
    val m = table.persist(Map("blobVal" -> wAry))

    table.tx {
      val found = table.find(where("blobVal = ?", wAry.array))
      assert(found.isDefined)
      assert(found.get[mutable.WrappedArray[Byte]]("blobVal") === wAry)
      assert(new String(found.get[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }

  it should " support inputstream result " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val bytes = testStr.getBytes
    table.persist(Map("blobVal" -> bytes))

    table.tx {
      val found = table.find(where("blobVal = ?", bytes))
      assert(found.isDefined)
      val is = found.get[InputStream]("blobVal")
      val readBytes = new Array[Byte](bytes.length)
      val dataIs = new DataInputStream(is)
      dataIs.readFully(readBytes)
      assert(readBytes === bytes)
      assert(new String(readBytes) === testStr)
    }

  }

}
