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

package org.apache.paimon.spark

import org.apache.paimon.spark.catalog.SupportFluss
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.lang.reflect.{Field, InvocationHandler, Method, Proxy}
import java.util.{Collections, HashMap, Map => JMap, Set => JSet}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

/** Tests Fluss LakeStream delegation through the Paimon Spark catalog. */
abstract class FlussCatalogTestBase extends PaimonSparkTestBase {

  private val tableIdentifier = Identifier.of(Array("db"), "orders")

  test("map Fluss options and initialize the delegate lazily") {
    val catalog = new TestingTableCatalog
    val loadedClassLoader = new AtomicReference[ClassLoader]
    val options = new HashMap[String, String]
    options.put("warehouse", "/tmp/warehouse")
    options.put("fluss.bootstrap.servers", "localhost:9123")
    options.put("fluss.client.security.protocol", "sasl")
    val delegate = new FlussCatalogDelegate(
      options,
      "paimon",
      new FlussCatalogDelegate.CatalogLoader {
        override def load(classLoader: ClassLoader): TableCatalog = {
          loadedClassLoader.set(classLoader)
          catalog
        }
      }
    )

    assert(
      delegate.flussOptions().asScala == Map(
        "bootstrap.servers" -> "localhost:9123",
        "client.security.protocol" -> "sasl"))
    assert(delegate.loadTable(tableIdentifier) eq catalog.table)
    assert(catalog.catalogName == "paimon")
    assert(
      catalog.options.asCaseSensitiveMap().asScala == Map(
        "bootstrap.servers" -> "localhost:9123",
        "client.security.protocol" -> "sasl"))
    assert(loadedClassLoader.get() != null)

    delegate.loadTable(tableIdentifier)
    assert(catalog.initializeCount == 1)
  }

  test("require Fluss bootstrap servers") {
    val delegate = new FlussCatalogDelegate(Collections.emptyMap[String, String](), "paimon")

    val error = intercept[IllegalStateException] {
      delegate.loadTable(tableIdentifier)
    }
    assert(error.getMessage.contains("fluss.bootstrap.servers"))
  }

  test("route a marked file store table to Fluss") {
    val paimonTable =
      SparkTable(fileStoreTable(Collections.singletonMap(SupportFluss.LAKESTREAM_ENABLED, "true")))
    val flussCatalog = new TestingTableCatalog
    val catalog = testingSparkCatalog(paimonTable, configuredDelegate(flussCatalog))

    assert(catalog.loadTable(tableIdentifier) eq flussCatalog.table)
    assert(flussCatalog.initializeCount == 1)
  }

  test("keep a regular file store table on Paimon without loading Fluss") {
    val paimonTable = SparkTable(fileStoreTable(Collections.emptyMap[String, String]()))
    val catalog = testingSparkCatalog(paimonTable, missingFlussDelegate)

    assert(catalog.loadTable(tableIdentifier) eq paimonTable)
  }

  test("report a missing Fluss connector for a marked table") {
    val paimonTable =
      SparkTable(fileStoreTable(Collections.singletonMap(SupportFluss.LAKESTREAM_ENABLED, "true")))
    val catalog = testingSparkCatalog(paimonTable, missingFlussDelegate)

    val error = intercept[IllegalStateException] {
      catalog.loadTable(tableIdentifier)
    }
    assert(error.getMessage.contains("matching the Spark version"))
    assert(error.getCause.isInstanceOf[ClassNotFoundException])
  }

  test("do not route a non-file-store table carrying the marker") {
    val systemTable =
      new TestingTable(Collections.singletonMap(SupportFluss.LAKESTREAM_ENABLED, "true"))
    val flussCatalog = new TestingTableCatalog
    val catalog = testingSparkCatalog(systemTable, configuredDelegate(flussCatalog))

    assert(catalog.loadTable(tableIdentifier) eq systemTable)
    assert(flussCatalog.initializeCount == 0)
  }

  private def configuredDelegate(catalog: TableCatalog): FlussCatalogDelegate =
    new FlussCatalogDelegate(
      Collections.singletonMap("fluss.bootstrap.servers", "localhost:9123"),
      "paimon",
      new FlussCatalogDelegate.CatalogLoader {
        override def load(classLoader: ClassLoader): TableCatalog = catalog
      }
    )

  private def missingFlussDelegate: FlussCatalogDelegate =
    new FlussCatalogDelegate(
      Collections.singletonMap("fluss.bootstrap.servers", "localhost:9123"),
      "paimon",
      new FlussCatalogDelegate.CatalogLoader {
        override def load(classLoader: ClassLoader): TableCatalog =
          throw new ClassNotFoundException("org.apache.fluss.spark.SparkCatalog")
      }
    )

  private def testingSparkCatalog(table: Table, delegate: FlussCatalogDelegate): SparkCatalog = {
    val catalog = new TestingSparkCatalog(table)
    val field: Field = classOf[SparkCatalog].getDeclaredField("flussCatalogDelegate")
    field.setAccessible(true)
    field.set(catalog, delegate)
    catalog
  }

  private def fileStoreTable(options: JMap[String, String]): FileStoreTable =
    Proxy
      .newProxyInstance(
        classOf[FileStoreTable].getClassLoader,
        Array(classOf[FileStoreTable]),
        new InvocationHandler {
          override def invoke(proxy: Object, method: Method, args: Array[Object]): Object =
            method.getName match {
              case "options" => options
              case "toString" => "TestingFileStoreTable"
              case _ => throw new UnsupportedOperationException(method.toString)
            }
        }
      )
      .asInstanceOf[FileStoreTable]

  private class TestingSparkCatalog(table: Table) extends SparkCatalog {

    override protected def loadSparkTable(
        ident: Identifier,
        extraOptions: JMap[String, String]): Table = table
  }

  private class TestingTable(tableProperties: JMap[String, String]) extends Table {

    override def name(): String = "orders"

    override def schema(): StructType = new StructType

    override def capabilities(): JSet[TableCapability] =
      Collections.emptySet[TableCapability]()

    override def properties(): JMap[String, String] = tableProperties
  }

  private class TestingTableCatalog extends TableCatalog {

    val table: Table = new TestingTable(Collections.emptyMap[String, String]())

    var catalogName: String = _
    var options: CaseInsensitiveStringMap = _
    var initializeCount: Int = 0
    var lastIdentifier: Identifier = _

    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
      catalogName = name
      this.options = options
      initializeCount += 1
    }

    override def name(): String = catalogName

    override def listTables(namespace: Array[String]): Array[Identifier] =
      Array(tableIdentifier)

    override def loadTable(identifier: Identifier): Table = {
      lastIdentifier = identifier
      table
    }

    override def createTable(
        identifier: Identifier,
        schema: StructType,
        partitions: Array[Transform],
        properties: JMap[String, String]): Table =
      throw new UnsupportedOperationException

    override def alterTable(identifier: Identifier, changes: TableChange*): Table =
      throw new UnsupportedOperationException

    override def dropTable(identifier: Identifier): Boolean =
      throw new UnsupportedOperationException

    override def renameTable(oldIdentifier: Identifier, newIdentifier: Identifier): Unit =
      throw new UnsupportedOperationException
  }
}
