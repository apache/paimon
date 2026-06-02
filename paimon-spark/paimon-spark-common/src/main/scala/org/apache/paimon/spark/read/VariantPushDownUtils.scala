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

import org.apache.paimon.data.variant.VariantMetadataUtils
import org.apache.paimon.types.{DataField, RowType}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Variant pushdown business logic, shared across Spark profiles. */
object VariantPushDownUtils {

  /**
   * Group extractions by column path. Whole-column rule: if any extraction on a path targets the
   * full Variant type, reject the whole path — Paimon can't reconstruct a Variant from shredded
   * fields, and Spark expects per-column acceptance to be all-or-nothing. Within-path order is
   * preserved (it maps to the projected struct's field index).
   */
  def acceptByPath(extractions: IndexedSeq[(Seq[String], VariantExtractionInfo, Boolean)])
      : (Map[Seq[String], Seq[VariantExtractionInfo]], Array[Boolean]) = {
    val byPath = mutable.LinkedHashMap.empty[Seq[String], mutable.ArrayBuffer[Int]]
    val rejected = mutable.HashSet.empty[Seq[String]]

    var i = 0
    while (i < extractions.length) {
      val (path, _, isVariantTarget) = extractions(i)
      if (path.isEmpty || isVariantTarget) {
        if (path.nonEmpty) {
          rejected += path
        }
      } else {
        byPath.getOrElseUpdate(path, mutable.ArrayBuffer.empty).append(i)
      }
      i += 1
    }

    val accepted = new Array[Boolean](extractions.length)
    val out = mutable.LinkedHashMap.empty[Seq[String], Seq[VariantExtractionInfo]]
    byPath.foreach {
      case (path, indices) =>
        if (!rejected.contains(path)) {
          val infos = indices.iterator.map {
            idx =>
              accepted(idx) = true
              extractions(idx)._2
          }.toVector
          out(path) = infos
        }
    }
    (out.toMap, accepted)
  }

  /**
   * Replace each variant field at a path in `accepted` with a Paimon variant `RowType`; recurse
   * into nested structs.
   */
  def rewriteRowType(rt: RowType, accepted: Map[Seq[String], Seq[VariantExtractionInfo]]): RowType =
    rewriteRowType(rt, accepted, Vector.empty)

  private def rewriteRowType(
      rt: RowType,
      accepted: Map[Seq[String], Seq[VariantExtractionInfo]],
      prefix: Seq[String]): RowType = {
    val newFields = rt.getFields.asScala.map(f => rewriteField(f, accepted, prefix)).asJava
    new RowType(rt.isNullable, newFields)
  }

  private def rewriteField(
      f: DataField,
      accepted: Map[Seq[String], Seq[VariantExtractionInfo]],
      prefix: Seq[String]): DataField = {
    val path = prefix :+ f.name()
    accepted.get(path) match {
      case Some(infos) =>
        val builder = VariantMetadataUtils.VariantRowTypeBuilder.builder(f.`type`().isNullable)
        infos.foreach(i => builder.field(i.paimonType, i.path, i.failOnError, i.timeZoneId))
        f.newType(builder.build())
      case None =>
        f.`type`() match {
          case nested: RowType => f.newType(rewriteRowType(nested, accepted, path))
          case _ => f
        }
    }
  }

  /** "col=[paths], nested.col=[paths]" rendering for `Scan.description()`. */
  def describeRewrittenRowType(rt: RowType): Option[String] = {
    val parts = mutable.ArrayBuffer.empty[String]
    collectVariantPaths(rt, Vector.empty, parts)
    if (parts.isEmpty) None else Some(parts.mkString(", "))
  }

  private def collectVariantPaths(
      rt: RowType,
      prefix: Seq[String],
      out: mutable.ArrayBuffer[String]): Unit = {
    rt.getFields.asScala.foreach {
      f =>
        val path = prefix :+ f.name()
        f.`type`() match {
          case child: RowType if VariantMetadataUtils.isVariantRowType(child) =>
            val paths = child.getFields.asScala.map(c => VariantMetadataUtils.path(c.description()))
            out.append(s"${path.mkString(".")}=[${paths.mkString(",")}]")
          case child: RowType =>
            collectVariantPaths(child, path, out)
          case _ =>
        }
    }
  }
}
