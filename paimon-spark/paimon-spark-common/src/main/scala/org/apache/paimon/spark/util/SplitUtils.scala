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

package org.apache.paimon.spark.util

import org.apache.paimon.table.format.FormatDataSplit
import org.apache.paimon.table.source.{DataSplit, Split}

import java.util.{Collections => JCollections}

import scala.collection.JavaConverters._

object SplitUtils {

  def splitSize(split: Split): Long = {
    split match {
      case ds: DataSplit =>
        ds.dataFiles().asScala.map(_.fileSize).sum
      case fs: FormatDataSplit =>
        if (fs.length() == null) fs.fileSize() else fs.length().longValue()
      case _ => 0
    }
  }

  def fileCount(split: Split): Long = dataFileCount(split) + deleteFileCount(split)

  def dataFileCount(split: Split): Long = {
    split match {
      case ds: DataSplit => ds.dataFiles().size()
      case _: FormatDataSplit => 1
      case _ => 0
    }
  }

  def deleteFileCount(split: Split): Long = {
    split match {
      case ds: DataSplit =>
        ds.deletionFiles()
          .orElse(JCollections.emptyList())
          .asScala
          .filter(_ != null)
          .map(_.path())
          .distinct
          .size
      case _ => 0
    }
  }
}
