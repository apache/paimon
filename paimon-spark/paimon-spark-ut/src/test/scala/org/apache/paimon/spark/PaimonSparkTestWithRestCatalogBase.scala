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

import org.apache.paimon.options.CatalogOptions
import org.apache.paimon.rest.{RESTCatalogFactory, RESTCatalogInternalOptions, RESTCatalogServer}
import org.apache.paimon.rest.auth.{AuthProviderEnum, BearTokenAuthProvider}
import org.apache.paimon.rest.responses.ConfigResponse
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions

import java.util.UUID

class PaimonSparkTestWithRestCatalogBase extends PaimonSparkTestBase {

  private var restCatalogServer: RESTCatalogServer = _
  private var serverUrl: String = _
  protected var warehouse: String = _
  private val initToken = "init_token"

  override protected def beforeAll(): Unit = {
    warehouse = UUID.randomUUID.toString
    val config = new ConfigResponse(
      ImmutableMap.of(
        RESTCatalogInternalOptions.PREFIX.key,
        "paimon",
        CatalogOptions.WAREHOUSE.key,
        warehouse),
      ImmutableMap.of())
    val authProvider = new BearTokenAuthProvider(initToken)
    restCatalogServer =
      new RESTCatalogServer(tempDBDir.getCanonicalPath, authProvider, config, warehouse)
    restCatalogServer.start()
    serverUrl = restCatalogServer.getUrl
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      restCatalogServer.shutdown()
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.paimon.metastore", RESTCatalogFactory.IDENTIFIER)
      .set("spark.sql.catalog.paimon.uri", serverUrl)
      .set("spark.sql.catalog.paimon.token", initToken)
      .set("spark.sql.catalog.paimon.warehouse", warehouse)
      .set("spark.sql.catalog.paimon.token.provider", AuthProviderEnum.BEAR.identifier)
  }
}
