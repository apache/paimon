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

package org.apache.spark.sql.connector.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.control.NonFatal

object PaimonCatalogUtils {

  def buildExternalCatalog(conf: SparkConf, hadoopConf: Configuration): ExternalCatalog = {
    val externalCatalogClassName =
      if (SparkSession.active.conf.get(CATALOG_IMPLEMENTATION.key).equals("hive")) {
        "org.apache.spark.sql.hive.HiveExternalCatalog"
      } else {
        "org.apache.spark.sql.catalyst.catalog.InMemoryCatalog"
      }
    reflect[ExternalCatalog, SparkConf, Configuration](externalCatalogClassName, conf, hadoopConf)
  }

  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
      className: String,
      ctorArg1: Arg1,
      ctorArg2: Arg2)(implicit ctorArgTag1: ClassTag[Arg1], ctorArgTag2: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

}
