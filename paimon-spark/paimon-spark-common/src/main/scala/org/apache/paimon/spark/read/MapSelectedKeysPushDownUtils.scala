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

package org.apache.paimon.spark.read

import org.apache.paimon.data.shredding.MapSelectedKeysMetadataUtils
import org.apache.paimon.types.{DataField, MapType, RowType}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** MAP selected-key pushdown business logic. */
object MapSelectedKeysPushDownUtils {

  /** Replace accepted MAP fields with selected-key ROW fields in the temporary read type. */
  def rewriteRowType(rt: RowType, accepted: Map[Seq[String], Seq[String]]): RowType =
    rewriteRowType(rt, accepted, Vector.empty)

  private def rewriteRowType(
      rt: RowType,
      accepted: Map[Seq[String], Seq[String]],
      prefix: Seq[String]): RowType = {
    val newFields = rt.getFields.asScala.map(f => rewriteField(f, accepted, prefix)).asJava
    new RowType(rt.isNullable, newFields)
  }

  private def rewriteField(
      field: DataField,
      accepted: Map[Seq[String], Seq[String]],
      prefix: Seq[String]): DataField = {
    val path = prefix :+ field.name()
    accepted.get(path) match {
      case Some(keys) =>
        field.`type`() match {
          case mapType: MapType =>
            val selectedFields = keys.zipWithIndex.map {
              case (_, ordinal) =>
                new DataField(ordinal, ordinal.toString, mapType.getValueType.copy(true))
            }.asJava
            MapSelectedKeysMetadataUtils.withSelectedKeys(
              field,
              new RowType(field.`type`().isNullable, selectedFields),
              keys.asJava)
          case _ => field
        }
      case None =>
        field.`type`() match {
          case nested: RowType => field.newType(rewriteRowType(nested, accepted, path))
          case _ => field
        }
    }
  }

  /** "col=[key1,key2], nested.col=[key1]" rendering for `Scan.description()`. */
  def describeRewrittenRowType(rt: RowType): Option[String] = {
    val parts = mutable.ArrayBuffer.empty[String]
    collectSelectedKeys(rt, Vector.empty, parts)
    if (parts.isEmpty) None else Some(parts.mkString(", "))
  }

  private def collectSelectedKeys(
      rt: RowType,
      prefix: Seq[String],
      out: mutable.ArrayBuffer[String]): Unit = {
    rt.getFields.asScala.foreach {
      field =>
        val path = prefix :+ field.name()
        if (MapSelectedKeysMetadataUtils.isMapSelectedKeysField(field)) {
          val keys = MapSelectedKeysMetadataUtils.selectedKeys(field.description()).asScala
          out.append(s"${path.mkString(".")}=[${keys.mkString(",")}]")
        } else {
          field.`type`() match {
            case child: RowType => collectSelectedKeys(child, path, out)
            case _ =>
          }
        }
    }
  }
}
