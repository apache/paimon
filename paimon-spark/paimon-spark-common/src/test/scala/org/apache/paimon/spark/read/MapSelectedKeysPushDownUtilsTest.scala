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
import org.apache.paimon.types.{DataTypes, MapType, RowType}

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** Tests for {@link MapSelectedKeysPushDownUtils}. */
class MapSelectedKeysPushDownUtilsTest extends AnyFunSuite {

  test("rewrite map field with selected keys") {
    val rowType = DataTypes.ROW(
      DataTypes.FIELD(0, "id", DataTypes.INT()),
      DataTypes.FIELD(1, "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT().notNull())),
      DataTypes.FIELD(2, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
    )

    val rewritten = MapSelectedKeysPushDownUtils.rewriteRowType(
      rowType,
      Map(Seq("attrs") -> Seq("key1", "key2"))
    )

    assert(rewritten.getField("id").`type`() == DataTypes.INT())
    assert(rewritten.getField("tags").`type`().isInstanceOf[MapType])

    val attrs = rewritten.getField("attrs")
    assert(attrs.description() == "__PAIMON_MAP_SELECTED_KEYS:key1;key2")
    assert(MapSelectedKeysMetadataUtils.isMapSelectedKeysField(attrs))
    assert(
      MapSelectedKeysMetadataUtils.selectedKeys(attrs.description()).asScala == Seq("key1", "key2"))

    val attrsRowType = attrs.`type`().asInstanceOf[RowType]
    assert(attrsRowType.getFieldNames.asScala == Seq("0", "1"))
    assert(attrsRowType.getField("0").`type`() == DataTypes.BIGINT())
    assert(attrsRowType.getField("1").`type`() == DataTypes.BIGINT())
    assert(attrsRowType.getField("0").`type`().isNullable)
    assert(attrsRowType.getField("1").`type`().isNullable)
  }

  test("rewrite nested map field with selected keys") {
    val nestedType = DataTypes.ROW(
      DataTypes.FIELD(2, "name", DataTypes.STRING()),
      DataTypes.FIELD(3, "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()))
    )
    val rowType = DataTypes.ROW(
      DataTypes.FIELD(0, "id", DataTypes.INT()),
      DataTypes.FIELD(1, "profile", nestedType)
    )

    val rewritten = MapSelectedKeysPushDownUtils.rewriteRowType(
      rowType,
      Map(Seq("profile", "attrs") -> Seq("key1", "key2"))
    )

    val profile = rewritten.getField("profile").`type`().asInstanceOf[RowType]
    assert(profile.getField("name").`type`() == DataTypes.STRING())

    val attrs = profile.getField("attrs")
    assert(attrs.description() == "__PAIMON_MAP_SELECTED_KEYS:key1;key2")
    assert(
      MapSelectedKeysMetadataUtils.selectedKeys(attrs.description()).asScala == Seq("key1", "key2"))

    val attrsRowType = attrs.`type`().asInstanceOf[RowType]
    assert(attrsRowType.getFieldNames.asScala == Seq("0", "1"))
    assert(attrsRowType.getField("0").`type`() == DataTypes.DOUBLE())
    assert(attrsRowType.getField("1").`type`() == DataTypes.DOUBLE())
  }
}
