/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.codegen

import org.apache.paimon.data._
import org.apache.paimon.data.variant.Variant
import org.apache.paimon.memory.MemorySegment
import org.apache.paimon.types._
import org.apache.paimon.types.DataTypeChecks.{getFieldCount, getFieldTypes, getPrecision, getScale}
import org.apache.paimon.types.DataTypeRoot._
import org.apache.paimon.utils.TypeCheckUtils._

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

/** Utilities to generate code for general purpose. */
object GenerateUtils {

  val DEFAULT_INPUT1_TERM = "in1"

  val DEFAULT_OUT_RECORD_TERM = "out"

  val DEFAULT_OUT_RECORD_WRITER_TERM = "outWriter"

  val ARRAY_DATA: String = className[InternalArray]

  val MAP_DATA: String = className[InternalMap]

  val ROW_DATA: String = className[InternalRow]

  val BINARY_ARRAY: String = className[BinaryArray]

  val BINARY_ROW: String = className[BinaryRow]

  val BINARY_STRING: String = className[BinaryString]

  val SEGMENT: String = className[MemorySegment]

  /** Retrieve the canonical name of a class type. */
  def className[T](implicit m: Manifest[T]): String = {
    val name = m.runtimeClass.getCanonicalName
    if (name == null) {
      throw new CodeGenException(
        s"Class '${m.runtimeClass.getName}' does not have a canonical name. " +
          s"Make sure it is statically accessible.")
    }
    name
  }

  /** Gets the default value for a primitive type, and null for generic types */
  def primitiveDefaultValue(t: DataType): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR => s"$BINARY_STRING.EMPTY_UTF8"
    case BOOLEAN => "false"
    case TINYINT | SMALLINT | INTEGER | DATE | TIME_WITHOUT_TIME_ZONE => "-1"
    case BIGINT => "-1L"
    case FLOAT => "-1.0f"
    case DOUBLE => "-1.0d"

    case _ => "null"
  }

  def generateFieldAccess(
      ctx: CodeGeneratorContext,
      inputType: DataType,
      inputTerm: String,
      index: Int): GeneratedExpression = inputType.getTypeRoot match {
    // ordered by type root definition
    case ROW =>
      val fieldType = getFieldTypes(inputType).get(index)
      val resultTypeTerm = primitiveTypeTermForType(fieldType)
      val defaultValue = primitiveDefaultValue(fieldType)
      val readCode = rowFieldReadAccess(index.toString, inputTerm, fieldType)
      val Seq(fieldTerm, nullTerm) =
        ctx.addReusableLocalVariables((resultTypeTerm, "field"), ("boolean", "isNull"))

      val inputCode =
        s"""
           |$nullTerm = $inputTerm.isNullAt($index);
           |$fieldTerm = $defaultValue;
           |if (!$nullTerm) {
           |  $fieldTerm = $readCode;
           |}
           """.stripMargin.trim

      GeneratedExpression(fieldTerm, nullTerm, inputCode, fieldType)

    case _ =>
      val fieldTypeTerm = boxedTypeTermForType(inputType)
      val inputCode = s"($fieldTypeTerm) $inputTerm"
      generateInputFieldUnboxing(ctx, inputType, inputCode, inputCode)
  }

  /** Generates code for comparing two fields. */
  def generateCompare(
      ctx: CodeGeneratorContext,
      t: DataType,
      nullsIsLast: Boolean,
      leftTerm: String,
      rightTerm: String): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR | DECIMAL | TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$leftTerm.compareTo($rightTerm)"
    case BOOLEAN =>
      s"($leftTerm == $rightTerm ? 0 : ($leftTerm ? 1 : -1))"
    case BINARY | VARBINARY =>
      val sortUtil =
        classOf[org.apache.paimon.utils.SortUtil].getCanonicalName
      s"$sortUtil.compareBinary($leftTerm, $rightTerm)"
    case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DATE | TIME_WITHOUT_TIME_ZONE =>
      s"($leftTerm > $rightTerm ? 1 : $leftTerm < $rightTerm ? -1 : 0)"
    case ARRAY =>
      val at = t.asInstanceOf[ArrayType]
      val compareFunc = newName("compareArray")
      val compareCode = generateArrayCompare(ctx, nullsIsLast = false, at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($ARRAY_DATA a, $ARRAY_DATA b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case MAP =>
      val at = t.asInstanceOf[MapType]
      val compareFunc = newName("compareMap")
      val compareCode = generateMapCompare(ctx, nullsIsLast = false, at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($MAP_DATA a, $MAP_DATA b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case MULTISET =>
      val at = t.asInstanceOf[MultisetType]
      val compareFunc = newName("compareMultiset")
      val compareCode = generateMultisetCompare(ctx, nullsIsLast = false, at, "a", "b")
      val funcCode: String =
        s"""
          public int $compareFunc($MAP_DATA a, $MAP_DATA b) {
            $compareCode
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case ROW =>
      val fieldCount = getFieldCount(t)
      val comparisons =
        generateRowCompare(ctx, t, getAscendingSortSpec((0 until fieldCount).toArray), "a", "b")
      val compareFunc = newName("compareRow")
      val funcCode: String =
        s"""
          public int $compareFunc($ROW_DATA a, $ROW_DATA b) {
            $comparisons
            return 0;
          }
        """
      ctx.addReusableMember(funcCode)
      s"$compareFunc($leftTerm, $rightTerm)"
    case _ =>
      throw new IllegalArgumentException("Illegal type: " + t)
  }

  /** Generates code for comparing array. */
  def generateArrayCompare(
      ctx: CodeGeneratorContext,
      nullsIsLast: Boolean,
      arrayType: ArrayType,
      leftTerm: String,
      rightTerm: String): String = {
    val nullIsLastRet = if (nullsIsLast) 1 else -1
    val elementType = arrayType.getElementType
    val fieldA = newName("fieldA")
    val isNullA = newName("isNullA")
    val lengthA = newName("lengthA")
    val fieldB = newName("fieldB")
    val isNullB = newName("isNullB")
    val lengthB = newName("lengthB")
    val minLength = newName("minLength")
    val i = newName("i")
    val comp = newName("comp")
    val typeTerm = primitiveTypeTermForType(elementType)
    s"""
        int $lengthA = $leftTerm.size();
        int $lengthB = $rightTerm.size();
        int $minLength = ($lengthA > $lengthB) ? $lengthB : $lengthA;
        for (int $i = 0; $i < $minLength; $i++) {
          boolean $isNullA = $leftTerm.isNullAt($i);
          boolean $isNullB = $rightTerm.isNullAt($i);
          if ($isNullA && $isNullB) {
            // Continue to compare the next element
          } else if ($isNullA) {
            return $nullIsLastRet;
          } else if ($isNullB) {
            return ${-nullIsLastRet};
          } else {
            $typeTerm $fieldA = ${rowFieldReadAccess(i, leftTerm, elementType)};
            $typeTerm $fieldB = ${rowFieldReadAccess(i, rightTerm, elementType)};
            int $comp = ${generateCompare(ctx, elementType, nullsIsLast, fieldA, fieldB)};
            if ($comp != 0) {
              return $comp;
            }
          }
        }

        if ($lengthA < $lengthB) {
          return -1;
        } else if ($lengthA > $lengthB) {
          return 1;
        }
      """
  }

  /** Generates code for comparing map. */
  def generateMapCompare(
      ctx: CodeGeneratorContext,
      nullsIsLast: Boolean,
      mapType: MapType,
      leftTerm: String,
      rightTerm: String): String = {
    val keyArrayType = new ArrayType(mapType.getKeyType)
    val valueArrayType = new ArrayType(mapType.getKeyType)
    generateMapDataCompare(ctx, nullsIsLast, leftTerm, rightTerm, keyArrayType, valueArrayType)
  }

  /** Generates code for comparing multiset. */
  def generateMultisetCompare(
      ctx: CodeGeneratorContext,
      nullsIsLast: Boolean,
      multisetType: MultisetType,
      leftTerm: String,
      rightTerm: String): String = {
    val keyArrayType = new ArrayType(multisetType.getElementType)
    val valueArrayType = new ArrayType(new IntType(false))
    generateMapDataCompare(ctx, nullsIsLast, leftTerm, rightTerm, keyArrayType, valueArrayType)
  }

  def generateMapDataCompare(
      ctx: CodeGeneratorContext,
      nullsIsLast: Boolean,
      leftTerm: String,
      rightTerm: String,
      keyArrayType: ArrayType,
      valueArrayType: ArrayType): String = {
    val keyArrayTerm = primitiveTypeTermForType(keyArrayType)
    val valueArrayTerm = primitiveTypeTermForType(valueArrayType)
    val lengthA = newName("lengthA")
    val lengthB = newName("lengthB")
    val comp = newName("comp")
    val keyArrayA = newName("keyArrayA")
    val keyArrayB = newName("keyArrayB")
    val valueArrayA = newName("valueArrayA")
    val valueArrayB = newName("valueArrayB")
    s"""
        int $lengthA = $leftTerm.size();
        int $lengthB = $rightTerm.size();
        if ($lengthA == $lengthB) {
          $keyArrayTerm $keyArrayA = $leftTerm.keyArray();
          $keyArrayTerm $keyArrayB = $rightTerm.keyArray();
          int $comp = ${generateCompare(ctx, keyArrayType, nullsIsLast, keyArrayA, keyArrayB)};
          if ($comp == 0) {
            $valueArrayTerm $valueArrayA = $leftTerm.valueArray();
            $valueArrayTerm $valueArrayB = $rightTerm.valueArray();
            $comp = ${generateCompare(ctx, valueArrayType, nullsIsLast, valueArrayA, valueArrayB)};
            if ($comp != 0) {
              return $comp;
            }
          } else {
            return $comp;
          }
        } else if ($lengthA < $lengthB) {
          return -1;
        } else if ($lengthA > $lengthB) {
          return 1;
        }
     """
  }

  /** Generates code for comparing row keys. */
  def generateRowCompare(
      ctx: CodeGeneratorContext,
      inputType: DataType,
      sortSpec: SortSpec,
      leftTerm: String,
      rightTerm: String): String = {

    val fieldTypes = getFieldTypes(inputType)
    val compares = new mutable.ArrayBuffer[String]
    sortSpec.getFieldSpecs.foreach {
      fieldSpec =>
        val index = fieldSpec.getFieldIndex
        val symbol = if (fieldSpec.getIsAscendingOrder) "" else "-"
        val nullIsLastRet = if (fieldSpec.getNullIsLast) 1 else -1
        val t = fieldTypes.get(index)

        val typeTerm = primitiveTypeTermForType(t)
        val fieldA = newName("fieldA")
        val isNullA = newName("isNullA")
        val fieldB = newName("fieldB")
        val isNullB = newName("isNullB")
        val comp = newName("comp")

        val code =
          s"""
             |boolean $isNullA = $leftTerm.isNullAt($index);
             |boolean $isNullB = $rightTerm.isNullAt($index);
             |if ($isNullA && $isNullB) {
             |  // Continue to compare the next element
             |} else if ($isNullA) {
             |  return $nullIsLastRet;
             |} else if ($isNullB) {
             |  return ${-nullIsLastRet};
             |} else {
             |  $typeTerm $fieldA = ${rowFieldReadAccess(index, leftTerm, t)};
             |  $typeTerm $fieldB = ${rowFieldReadAccess(index, rightTerm, t)};
             |  int $comp = ${generateCompare(ctx, t, fieldSpec.getNullIsLast, fieldA, fieldB)};
             |  if ($comp != 0) {
             |    return $symbol$comp;
             |  }
             |}
         """.stripMargin
        compares += code
    }
    compares.mkString
  }

  // when casting we first need to unbox Primitives, for example,
  // float a = 1.0f;
  // byte b = (byte) a;
  // works, but for boxed types we need this:
  // Float a = 1.0f;
  // Byte b = (byte)(float) a;
  def primitiveTypeTermForType(t: DataType): String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN => "boolean"
    case TINYINT => "byte"
    case SMALLINT => "short"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE => "int"
    case BIGINT => "long"
    case FLOAT => "float"
    case DOUBLE => "double"
    case _ => boxedTypeTermForType(t)
  }

  def boxedTypeTermForType(t: DataType): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR => BINARY_STRING
    case BOOLEAN => className[JBoolean]
    case BINARY | VARBINARY => "byte[]"
    case DECIMAL => className[Decimal]
    case TINYINT => className[JByte]
    case SMALLINT => className[JShort]
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE => className[JInt]
    case BIGINT => className[JLong]
    case FLOAT => className[JFloat]
    case DOUBLE => className[JDouble]
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE => className[Timestamp]
    case ARRAY => className[InternalArray]
    case MULTISET | MAP => className[InternalMap]
    case ROW => className[InternalRow]
    case VARIANT => className[Variant]
    case _ =>
      throw new IllegalArgumentException("Illegal type: " + t)
  }

  def rowFieldReadAccess(index: Int, rowTerm: String, fieldType: DataType): String =
    rowFieldReadAccess(index.toString, rowTerm, fieldType)

  def rowFieldReadAccess(indexTerm: String, rowTerm: String, t: DataType): String =
    t.getTypeRoot match {
      // ordered by type root definition
      case CHAR | VARCHAR =>
        s"(($BINARY_STRING) $rowTerm.getString($indexTerm))"
      case BOOLEAN =>
        s"$rowTerm.getBoolean($indexTerm)"
      case BINARY | VARBINARY =>
        s"$rowTerm.getBinary($indexTerm)"
      case DECIMAL =>
        s"$rowTerm.getDecimal($indexTerm, ${getPrecision(t)}, ${getScale(t)})"
      case TINYINT =>
        s"$rowTerm.getByte($indexTerm)"
      case SMALLINT =>
        s"$rowTerm.getShort($indexTerm)"
      case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
        s"$rowTerm.getInt($indexTerm)"
      case BIGINT =>
        s"$rowTerm.getLong($indexTerm)"
      case FLOAT =>
        s"$rowTerm.getFloat($indexTerm)"
      case DOUBLE =>
        s"$rowTerm.getDouble($indexTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        s"$rowTerm.getTimestamp($indexTerm, ${getPrecision(t)})"
      case ARRAY =>
        s"$rowTerm.getArray($indexTerm)"
      case MULTISET | MAP =>
        s"$rowTerm.getMap($indexTerm)"
      case ROW =>
        s"$rowTerm.getRow($indexTerm, ${getFieldCount(t)})"
      case VARIANT =>
        s"$rowTerm.getVariant($indexTerm)"
      case _ =>
        throw new IllegalArgumentException("Illegal type: " + t)
    }

  /**
   * Converts the external boxed format to an internal mostly primitive field representation.
   * Wrapper types can autoboxed to their corresponding primitive type (Integer -> int).
   *
   * @param ctx
   *   code generator context which maintains various code statements.
   * @param inputType
   *   type of field
   * @param inputTerm
   *   expression term of field to be unboxed
   * @param inputUnboxingTerm
   *   unboxing/conversion term
   * @return
   *   internal unboxed field representation
   */
  def generateInputFieldUnboxing(
      ctx: CodeGeneratorContext,
      inputType: DataType,
      inputTerm: String,
      inputUnboxingTerm: String): GeneratedExpression = {

    val resultTypeTerm = primitiveTypeTermForType(inputType)
    val defaultValue = primitiveDefaultValue(inputType)

    val Seq(resultTerm, nullTerm) =
      ctx.addReusableLocalVariables((resultTypeTerm, "result"), ("boolean", "isNull"))

    val wrappedCode =
      s"""
         |$nullTerm = $inputTerm == null;
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $resultTerm = $inputUnboxingTerm;
         |}
         |""".stripMargin.trim

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, inputType)
  }

  private val nameCounter = new AtomicLong

  def newName(name: String): String = {
    s"$name$$${nameCounter.getAndIncrement}"
  }

  def newNames(names: String*): Seq[String] = {
    require(names.toSet.size == names.length, "Duplicated names")
    val newId = nameCounter.getAndIncrement
    names.map(name => s"$name$$$newId")
  }

  def getAscendingSortSpec(fields: Array[Int]): SortSpec = {
    val originalOrders = fields.map(_ => true)
    val nullsIsLast = getNullDefaultOrders(originalOrders)
    deduplicateSortKeys(fields, originalOrders, nullsIsLast)
  }

  private def deduplicateSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): SortSpec = {
    val builder = SortSpec.builder()
    val keySet = new mutable.HashSet[Int]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        builder.addField(keys(i), orders(i), nullsIsLast(i))
      }
    }
    builder.build()
  }

  /** Returns the default null direction if not specified. */
  def getNullDefaultOrders(ascendings: Array[Boolean]): Array[Boolean] = {
    ascendings.map(asc => !asc)
  }

  def rowSetField(
      ctx: CodeGeneratorContext,
      rowClass: Class[_ <: InternalRow],
      rowTerm: String,
      indexTerm: String,
      fieldExpr: GeneratedExpression,
      binaryRowWriterTerm: Option[String]): String = {

    val fieldType = fieldExpr.resultType
    val fieldTerm = fieldExpr.resultTerm

    if (rowClass == classOf[BinaryRow]) {
      binaryRowWriterTerm match {
        case Some(writer) =>
          // use writer to set field
          val writeField = binaryWriterWriteField(ctx, indexTerm, fieldTerm, writer, fieldType)
          s"""
             |${fieldExpr.code}
             |if (${fieldExpr.nullTerm}) {
             |  ${binaryWriterWriteNull(indexTerm, writer, fieldType)};
             |} else {
             |  $writeField;
             |}
           """.stripMargin

        case None =>
          // directly set field to BinaryRowData, this depends on all the fields are fixed length
          val writeField = binaryRowFieldSetAccess(indexTerm, rowTerm, fieldType, fieldTerm)

          s"""
             |${fieldExpr.code}
             |if (${fieldExpr.nullTerm}) {
             |  ${binaryRowSetNull(indexTerm, rowTerm, fieldType)};
             |} else {
             |  $writeField;
             |}
           """.stripMargin
      }
    } else {
      throw new UnsupportedOperationException("Not support set field for " + rowClass)
    }
  }

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      index: Int,
      fieldValTerm: String,
      writerTerm: String,
      fieldType: DataType): String =
    binaryWriterWriteField(
      t => ctx.addReusableTypeSerializer(t),
      index.toString,
      fieldValTerm,
      writerTerm,
      fieldType)

  def binaryWriterWriteField(
      ctx: CodeGeneratorContext,
      indexTerm: String,
      fieldValTerm: String,
      writerTerm: String,
      t: DataType): String =
    binaryWriterWriteField(
      t => ctx.addReusableTypeSerializer(t),
      indexTerm,
      fieldValTerm,
      writerTerm,
      t)

  def binaryWriterWriteField(
      addSerializer: DataType => String,
      indexTerm: String,
      fieldValTerm: String,
      writerTerm: String,
      t: DataType): String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR =>
      s"$writerTerm.writeString($indexTerm, $fieldValTerm)"
    case BOOLEAN =>
      s"$writerTerm.writeBoolean($indexTerm, $fieldValTerm)"
    case BINARY | VARBINARY =>
      s"$writerTerm.writeBinary($indexTerm, $fieldValTerm)"
    case DECIMAL =>
      s"$writerTerm.writeDecimal($indexTerm, $fieldValTerm, ${getPrecision(t)})"
    case TINYINT =>
      s"$writerTerm.writeByte($indexTerm, $fieldValTerm)"
    case SMALLINT =>
      s"$writerTerm.writeShort($indexTerm, $fieldValTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
      s"$writerTerm.writeInt($indexTerm, $fieldValTerm)"
    case BIGINT =>
      s"$writerTerm.writeLong($indexTerm, $fieldValTerm)"
    case FLOAT =>
      s"$writerTerm.writeFloat($indexTerm, $fieldValTerm)"
    case DOUBLE =>
      s"$writerTerm.writeDouble($indexTerm, $fieldValTerm)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$writerTerm.writeTimestamp($indexTerm, $fieldValTerm, ${getPrecision(t)})"
    case ARRAY =>
      val ser = addSerializer(t)
      s"$writerTerm.writeArray($indexTerm, $fieldValTerm, $ser)"
    case MULTISET | MAP =>
      val ser = addSerializer(t)
      s"$writerTerm.writeMap($indexTerm, $fieldValTerm, $ser)"
    case ROW =>
      val ser = addSerializer(t)
      s"$writerTerm.writeRow($indexTerm, $fieldValTerm, $ser)"
    case _ =>
      throw new IllegalArgumentException("Illegal type: " + t);
  }

  def binaryWriterWriteNull(index: Int, writerTerm: String, t: DataType): String =
    binaryWriterWriteNull(index.toString, writerTerm, t)

  def binaryWriterWriteNull(indexTerm: String, writerTerm: String, t: DataType): String =
    t.getTypeRoot match {
      // ordered by type root definition
      case DECIMAL if !Decimal.isCompact(getPrecision(t)) =>
        s"$writerTerm.writeDecimal($indexTerm, null, ${getPrecision(t)})"
      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE
          if !Timestamp.isCompact(getPrecision(t)) =>
        s"$writerTerm.writeTimestamp($indexTerm, null, ${getPrecision(t)})"
      case _ =>
        s"$writerTerm.setNullAt($indexTerm)"
    }

  def boxedWrapperRowFieldSetAccess(
      rowTerm: String,
      indexTerm: String,
      fieldTerm: String,
      t: DataType): String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN =>
      s"$rowTerm.setBoolean($indexTerm, $fieldTerm)"
    case TINYINT =>
      s"$rowTerm.setByte($indexTerm, $fieldTerm)"
    case SMALLINT =>
      s"$rowTerm.setShort($indexTerm, $fieldTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
      s"$rowTerm.setInt($indexTerm, $fieldTerm)"
    case BIGINT =>
      s"$rowTerm.setLong($indexTerm, $fieldTerm)"
    case FLOAT =>
      s"$rowTerm.setFloat($indexTerm, $fieldTerm)"
    case DOUBLE =>
      s"$rowTerm.setDouble($indexTerm, $fieldTerm)"
    case _ =>
      s"$rowTerm.setNonPrimitiveValue($indexTerm, $fieldTerm)"
  }

  // -------------------------- BinaryArray Set Access -------------------------------

  def binaryArraySetNull(index: Int, arrayTerm: String, t: DataType): String =
    t.getTypeRoot match {
      // ordered by type root definition
      case BOOLEAN =>
        s"$arrayTerm.setNullBoolean($index)"
      case TINYINT =>
        s"$arrayTerm.setNullByte($index)"
      case SMALLINT =>
        s"$arrayTerm.setNullShort($index)"
      case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
        s"$arrayTerm.setNullInt($index)"
      case FLOAT =>
        s"$arrayTerm.setNullFloat($index)"
      case DOUBLE =>
        s"$arrayTerm.setNullDouble($index)"
      case _ =>
        s"$arrayTerm.setNullLong($index)"
    }

  def binaryRowFieldSetAccess(
      index: Int,
      binaryRowTerm: String,
      fieldType: DataType,
      fieldValTerm: String): String =
    binaryRowFieldSetAccess(index.toString, binaryRowTerm, fieldType, fieldValTerm)

  def binaryRowFieldSetAccess(
      index: String,
      binaryRowTerm: String,
      t: DataType,
      fieldValTerm: String): String = t.getTypeRoot match {
    // ordered by type root definition
    case BOOLEAN =>
      s"$binaryRowTerm.setBoolean($index, $fieldValTerm)"
    case DECIMAL =>
      s"$binaryRowTerm.setDecimal($index, $fieldValTerm, ${getPrecision(t)})"
    case TINYINT =>
      s"$binaryRowTerm.setByte($index, $fieldValTerm)"
    case SMALLINT =>
      s"$binaryRowTerm.setShort($index, $fieldValTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
      s"$binaryRowTerm.setInt($index, $fieldValTerm)"
    case BIGINT =>
      s"$binaryRowTerm.setLong($index, $fieldValTerm)"
    case FLOAT =>
      s"$binaryRowTerm.setFloat($index, $fieldValTerm)"
    case DOUBLE =>
      s"$binaryRowTerm.setDouble($index, $fieldValTerm)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$binaryRowTerm.setTimestamp($index, $fieldValTerm, ${getPrecision(t)})"
    case _ =>
      throw new CodeGenException(
        "Fail to find binary row field setter method of DataType " + t + ".")
  }

  def binaryRowSetNull(index: Int, rowTerm: String, t: DataType): String =
    binaryRowSetNull(index.toString, rowTerm, t)

  def binaryRowSetNull(indexTerm: String, rowTerm: String, t: DataType): String =
    t.getTypeRoot match {
      // ordered by type root definition
      case DECIMAL if !Decimal.isCompact(getPrecision(t)) =>
        s"$rowTerm.setDecimal($indexTerm, null, ${getPrecision(t)})"
      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE
          if !Timestamp.isCompact(getPrecision(t)) =>
        s"$rowTerm.setTimestamp($indexTerm, null, ${getPrecision(t)})"
      case _ =>
        s"$rowTerm.setNullAt($indexTerm)"
    }

  /**
   * Generates a record declaration statement, and add it to reusable member. The record can be any
   * type of RowData or other types.
   *
   * @param t
   *   the record type
   * @param clazz
   *   the specified class of the type (only used when RowType)
   * @param recordTerm
   *   the record term to be declared
   * @param recordWriterTerm
   *   the record writer term (only used when BinaryRowData type)
   * @param ctx
   *   the code generator context
   * @return
   *   the record initialization statement
   */
  def generateRecordStatement(
      t: DataType,
      clazz: Class[_],
      recordTerm: String,
      recordWriterTerm: Option[String] = None,
      ctx: CodeGeneratorContext): String = t.getTypeRoot match {
    // ordered by type root definition
    case ROW if clazz == classOf[BinaryRow] =>
      val writerTerm = recordWriterTerm.getOrElse(
        throw new CodeGenException("No writer is specified when writing BinaryRowData record.")
      )
      val binaryRowWriter = className[BinaryRowWriter]
      val typeTerm = clazz.getCanonicalName
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm(${getFieldCount(t)});")
      ctx.addReusableMember(s"$binaryRowWriter $writerTerm = new $binaryRowWriter($recordTerm);")
      s"""
         |$recordTerm = new $typeTerm(${getFieldCount(t)});
         |$writerTerm = new $binaryRowWriter($recordTerm);
         |""".stripMargin.trim
    case _ =>
      val typeTerm = boxedTypeTermForType(t)
      ctx.addReusableMember(s"$typeTerm $recordTerm = new $typeTerm();")
      s"$recordTerm = new $typeTerm();"
  }

  /** Generates a call with a single result statement. */
  def generateCallIfArgsNotNull(
      ctx: CodeGeneratorContext,
      returnType: DataType,
      operands: Seq[GeneratedExpression],
      resultNullable: Boolean = false,
      wrapTryCatch: Boolean = false)(call: Seq[String] => String): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable, wrapTryCatch) {
      args => ("", call(args))
    }
  }

  /** Generates a call with auxiliary statements and result expression. */
  def generateCallWithStmtIfArgsNotNull(
      ctx: CodeGeneratorContext,
      returnType: DataType,
      operands: Seq[GeneratedExpression],
      resultNullable: Boolean = false,
      wrapTryCatch: Boolean = false)(call: Seq[String] => (String, String)): GeneratedExpression = {
    val resultTypeTerm = if (resultNullable) {
      boxedTypeTermForType(returnType)
    } else {
      primitiveTypeTermForType(returnType)
    }
    val nullTerm = ctx.addReusableLocalVariable("boolean", "isNull")
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val defaultValue = primitiveDefaultValue(returnType)
    val isResultNullable = resultNullable || (isReference(returnType) && !isTemporal(returnType))
    val nullTermCode = if (isResultNullable) {
      s"$nullTerm = ($resultTerm == null);"
    } else {
      ""
    }

    val (stmt, result) = call(operands.map(_.resultTerm))

    val wrappedResultAssignment = if (wrapTryCatch) {
      s"""
         |try {
         |  $stmt
         |  $resultTerm = $result;
         |} catch (Throwable ${newName("ignored")}) {
         |  $nullTerm = true;
         |  $resultTerm = $defaultValue;
         |}
         |""".stripMargin
    } else {
      s"""
         |$stmt
         |$resultTerm = $result;
         |""".stripMargin
    }

    val resultCode = if (operands.nonEmpty) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $wrappedResultAssignment
         |  $nullTermCode
         |}
         |""".stripMargin
    } else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = false;
         |$wrappedResultAssignment
         |$nullTermCode
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
