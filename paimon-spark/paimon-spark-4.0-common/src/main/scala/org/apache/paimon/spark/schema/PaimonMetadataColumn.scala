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

package org.apache.paimon.spark.schema

import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.types.DataField

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField}

case class PaimonMetadataColumn(id: Int, override val name: String, override val dataType: DataType)
  extends MetadataColumn {

  def toPaimonDataField: DataField = {
    new DataField(id, name, SparkTypeUtils.toPaimonType(dataType));
  }

  def toStructField: StructField = {
    StructField(name, dataType);
  }

  def toAttribute: AttributeReference = {
    AttributeReference(name, dataType)()
  }
}

object PaimonMetadataColumn {

  val FILE_PATH_COLUMN = "__paimon_file_path"
  val ROW_INDEX_COLUMN = "__paimon_row_index"
  val SUPPORTED_METADATA_COLUMNS: Seq[String] = Seq(FILE_PATH_COLUMN, ROW_INDEX_COLUMN)

  val ROW_INDEX: PaimonMetadataColumn =
    PaimonMetadataColumn(Int.MaxValue - 100, ROW_INDEX_COLUMN, LongType)
  val FILE_PATH: PaimonMetadataColumn =
    PaimonMetadataColumn(Int.MaxValue - 101, FILE_PATH_COLUMN, StringType)

  def get(metadataColumn: String): PaimonMetadataColumn = {
    if (metadataColumn == FILE_PATH_COLUMN) {
      FILE_PATH
    } else if (metadataColumn == ROW_INDEX_COLUMN) {
      ROW_INDEX
    } else {
      throw new IllegalArgumentException(s"$metadataColumn metadata column is not supported.")
    }
  }
}
