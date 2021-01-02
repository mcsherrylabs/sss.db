package sss.db

import java.sql.Types._
import sss.ancillary.Serialize._


object RowSerializer {

  implicit class RowToBytes(val row: Row) {

    def toBytes(colTypes: ColumnsMetaInfo): Array[Byte] = {
      colTypes.foldLeft[ToBytes](EmptySerializer) {
        case (acc, ColumnMetaInfo(k, `CHAR` | `VARCHAR` | `LONGVARCHAR`, false)) => acc ++ OptionSerializer(row.stringOpt(k), StringSerializer)
        case (acc, ColumnMetaInfo(k, `CHAR` | `VARCHAR` | `LONGVARCHAR`, true)) => acc ++ StringSerializer(row.string(k))

        case (acc, ColumnMetaInfo(k, `BIT`, false)) => acc ++ OptionSerializer(row.booleanOpt(k), BooleanSerializer)
        case (acc, ColumnMetaInfo(k, `BIT`, true)) => acc ++ BooleanSerializer(row.boolean(k))
        //Note .getObject returns Integer instead of Byte for TinyInt
        /*case (acc, ColumnMetaInfo(k, `TINYINT`, false)) => acc ++ OptionSerializer(row.intOpt(k), IntSerializer)
        case (acc, ColumnMetaInfo(k, `TINYINT`, true)) => acc ++ IntSerializer(row.int(k))
        case (acc, ColumnMetaInfo(k, `SMALLINT`, false)) => acc ++ OptionSerializer(row.shortOpt(k), ShortSerializer)
        case (acc, ColumnMetaInfo(k, `SMALLINT`, true)) => acc ++ ShortSerializer(row.short(k))*/
        case (acc, ColumnMetaInfo(k, `INTEGER`, false)) => acc ++ OptionSerializer(row.intOpt(k), IntSerializer)
        case (acc, ColumnMetaInfo(k, `INTEGER`, true)) => acc ++ IntSerializer(row.int(k))
        case (acc, ColumnMetaInfo(k, `BIGINT`, false)) => acc ++ OptionSerializer(row.longOpt(k), LongSerializer)
        case (acc, ColumnMetaInfo(k, `BIGINT`, true)) => acc ++ LongSerializer(row.long(k))

        case (acc, ColumnMetaInfo(k, `BINARY` | `VARBINARY` | `LONGVARBINARY` , false)) => acc ++ OptionSerializer(row.arrayByteOpt(k), ByteArraySerializer)
        case (acc, ColumnMetaInfo(k, `BINARY` | `VARBINARY` | `LONGVARBINARY` , true)) => acc ++ ByteArraySerializer(row.arrayByte(k))

        case (acc, ColumnMetaInfo(k, `BLOB` , false)) => acc ++ OptionSerializer(row.blobInputStreamOpt(k), InputStreamSerializer)
        case (acc, ColumnMetaInfo(k, `BLOB` , true)) => acc ++ InputStreamSerializer(row.blobInputStream(k))

        case (acc, colInfo) => DbException(s"Unsupported type ${colInfo}")
      }.toBytes

    }
  }

  def rowFromBytes(colsInfo: ColumnsMetaInfo, bytes: Array[Byte]): Row = {

    def toMap(acc: Map[String, _], nextBytes: Array[Byte], remainingColsInfo: ColumnsMetaInfo): Map[String, _] = {
      remainingColsInfo.headOption match {
        case None => acc
        case Some(colInfo) =>
          val deserializer = getDeSerializeTarget(colInfo)

          val (value, remainBytes) = deserializer match {
            case opt: OptionDeSerialize[t] =>
              val (a, restBytes) = opt.extract(nextBytes)
              a.payload match {
                case Some(o) => (o, restBytes)
                case None => (null, restBytes)
              }

            case nonOpt =>
              val (deserialized, rest) = nonOpt.extract(nextBytes)
              (deserialized.payload, rest)
          }
          toMap(acc + (colInfo.name -> value), remainBytes, remainingColsInfo.tail)
      }
    }

    new Row(toMap(Map.empty[String, Any], bytes, colsInfo))
  }

  private def getDeSerializeTarget(colInfo: ColumnMetaInfo): DeSerializeTarget = colInfo match {

    case ColumnMetaInfo(k, `CHAR` | `VARCHAR` | `LONGVARCHAR`, false) => OptionDeSerialize(StringDeSerialize)
    case ColumnMetaInfo(k, `CHAR` | `VARCHAR` | `LONGVARCHAR`, true) => StringDeSerialize

    case ColumnMetaInfo(k, `BIT`, false) => OptionDeSerialize(BooleanDeSerialize)
    case ColumnMetaInfo(k, `BIT`, true) => BooleanDeSerialize
    case ColumnMetaInfo(k, `TINYINT`, false) => OptionDeSerialize(ByteDeSerialize)
    case ColumnMetaInfo(k, `TINYINT`, true) => ByteDeSerialize
    case ColumnMetaInfo(k, `SMALLINT`, false) => OptionDeSerialize(ShortDeSerialize)
    case ColumnMetaInfo(k, `SMALLINT`, true) => ShortDeSerialize
    case ColumnMetaInfo(k, `INTEGER`, false) => OptionDeSerialize(IntDeSerialize)
    case ColumnMetaInfo(k, `INTEGER`, true) => IntDeSerialize
    case ColumnMetaInfo(k, `BIGINT`, false) => OptionDeSerialize(LongDeSerialize)
    case ColumnMetaInfo(k, `BIGINT`, true) => LongDeSerialize

    case ColumnMetaInfo(k, `BINARY` | `VARBINARY` | `LONGVARBINARY`, false) => OptionDeSerialize(ByteArrayDeSerialize)
    case ColumnMetaInfo(k, `BINARY` | `VARBINARY` | `LONGVARBINARY`, true) => ByteArrayDeSerialize

    case ColumnMetaInfo(k, `BLOB`, false) => OptionDeSerialize(InputStreamDeSerialize)
    case ColumnMetaInfo(k, `BLOB`, true) => InputStreamDeSerialize

    case colInfo => DbException(s"Unsupported type ${colInfo}")
  }


}
