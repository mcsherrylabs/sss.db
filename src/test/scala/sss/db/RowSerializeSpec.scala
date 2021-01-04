package sss.db

import java.io.FileInputStream

import org.scalatest.DoNotDiscover
import RowSerializer._
import sss.db.BlobStoreHelper._

import scala.collection.immutable.ArraySeq

//import scala.collection.immutable.ArraySeq
import scala.util.Random

/**
  * Created by alan on 6/21/16.
  */
@DoNotDiscover
class RowSerializeSpec extends DbSpecSetup {

  def view = fixture.dbUnderTest.table(testRowSerialize)

  val int_col = "int_col"
  val int_col_opt = "int_col_opt"
  val char_col = "char_col"
  val varchar_col = "varchar_col"
  val longvarchar_col = "longvarchar_col"
  val char_col_opt = "char_col_opt"
  val varchar_col_opt = "varchar_col_opt"
  val longvarchar_col_opt = "longvarchar_col_opt"
  val bit_col = "bit_col"
  val bit_col_opt = "bit_col_opt"
  val tinyint_col = "tinyint_col"
  val tinyint_col_opt = "tinyint_col_opt"
  val smallint_col = "smallint_col"
  val smallint_col_opt = "smallint_col_opt"
  val bigint_col = "bigint_col"
  val bigint_col_opt = "bigint_col_opt"
  val binary_col = "binary_col "
  val binary_col_opt = "binary_col_opt "
  val varbinary_col = "varbinary_col "
  val varbinary_col_opt = "varbinary_col_opt "
  val longvarbinary_col = "longvarbinary_col"
  val longvarbinary_col_opt = "longvarbinary_col_opt "
  val blob_col = "blob_col"
  val blob_col_opt = "blob_col_opt"

  def randomByteArySeq(size: Int): ArraySeq[Byte] = {
    val result = Array.ofDim[Byte](size)
    Random.nextBytes(result)
    ArraySeq.from(result)
    //result
  }

  def randomByteAry(size: Int): Array[Byte] = {
    val result = Array.ofDim[Byte](size)
    Random.nextBytes(result)
    result
  }

  "A View" should "deserialize and serialize a row" in {

    val blobIn = createFile(1 * KB)
    blobIn.deleteOnExit()

    val db = fixture.dbUnderTest
    import db.runContext._

    val plan = view.insert(Map(
      int_col -> 34,
      int_col_opt -> None,
      char_col -> 'C',
      varchar_col -> "dsdfsdfsf",
      longvarchar_col -> "sdfsdfsdfsdf",
      char_col_opt -> None,
      varchar_col_opt -> None,
      longvarchar_col_opt -> None,
      bit_col -> true,
      bit_col_opt -> None,
      /*tinyint_col -> 5,
      tinyint_col_opt -> None,
      smallint_col -> 37,
      smallint_col_opt -> None,*/
      bigint_col -> Long.MaxValue,
      bigint_col_opt -> None,
      binary_col -> randomByteAry(30),
      binary_col_opt -> None,
      varbinary_col -> randomByteAry(30),
      varbinary_col_opt -> None,
      longvarbinary_col -> randomByteAry(30),
      longvarbinary_col_opt -> None,
      blob_col -> new FileInputStream(blobIn),
      blob_col_opt -> None,
    ))

    val row = plan.runSync.get
    val serialized = row.toBytes(view.columnsMetaInfo)

    val row2 = rowFromBytes(view.columnsMetaInfo, serialized)

    assert(row shallowEquals row2)

  }

  it should "deserialize and serialize a row where the optional columns are ignored" in {

    val blobIn = createFile(1 * KB)
    blobIn.deleteOnExit()

    val db = fixture.dbUnderTest
    import db.runContext._

    val row = view.insert(Map(
      int_col -> 34,
      char_col -> 'C',
      varchar_col -> "dsdfsdfsf",
      longvarchar_col -> "sdfsdfsdfsdf",
      bit_col -> true,
      bigint_col -> Long.MaxValue,
      binary_col -> randomByteArySeq(30),
      varbinary_col -> randomByteAry(30),
      longvarbinary_col -> randomByteArySeq(30),
      blob_col -> new FileInputStream(blobIn),

    )).runSync.get

    val serialized = row.toBytes(view.columnsMetaInfo)

    val row2 = rowFromBytes(view.columnsMetaInfo, serialized)

    assert(row shallowEquals row2)

  }

  it should "deserialize and serialize a row where the optional columns are set" in {

    val blobIn = createFile(1 * KB)
    blobIn.deleteOnExit()
    val db = fixture.dbUnderTest
    import db.runContext._

    val row = view.insert(Map(
      int_col -> 34,
      int_col_opt -> 45,
      char_col -> 'C',
      varchar_col -> "dsdfsdfsf",
      longvarchar_col -> "sdfsdfsdfsdf",
      char_col_opt -> 'F',
      varchar_col_opt -> "werwer235r2",
      longvarchar_col_opt -> "ert235r2r",
      bit_col -> true,
      bit_col_opt -> false,

      bigint_col -> Long.MaxValue,
      bigint_col_opt -> Int.MaxValue,
      binary_col -> randomByteAry(30),
      binary_col_opt -> randomByteAry(30),
      varbinary_col -> randomByteAry(30),
      varbinary_col_opt -> randomByteAry(30),
      longvarbinary_col -> randomByteAry(30),
      longvarbinary_col_opt -> randomByteAry(30),
      blob_col -> new FileInputStream(blobIn),
      blob_col_opt -> new FileInputStream(blobIn),
    )).runSync.get

    val serialized = row.toBytes(view.columnsMetaInfo)

    val row2 = rowFromBytes(view.columnsMetaInfo, serialized)

    assert(row.int(int_col) === row2.int(int_col))
    assert(row.intOpt(int_col_opt) === row2.intOpt(int_col_opt))
    assert(row.shallowEquals(row2))


  }
}
