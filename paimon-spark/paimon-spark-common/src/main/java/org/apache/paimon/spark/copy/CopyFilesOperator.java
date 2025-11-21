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

package org.apache.paimon.spark.copy;

import org.apache.paimon.catalog.Catalog;

import org.apache.spark.sql.SparkSession;

/** Base class for copy files. */
public abstract class CopyFilesOperator {

    protected final SparkSession spark;
    protected final Catalog sourceCatalog;
    protected final Catalog targetCatalog;

    public CopyFilesOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        this.spark = spark;
        this.sourceCatalog = sourceCatalog;
        this.targetCatalog = targetCatalog;
    }
}
