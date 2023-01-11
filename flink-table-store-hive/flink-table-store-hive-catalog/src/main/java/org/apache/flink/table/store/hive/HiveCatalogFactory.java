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

package org.apache.flink.table.store.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.flink.table.store.CatalogOptions.URI;

/** Factory to create {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {

    private static final String IDENTIFIER = "hive";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(String warehouse, Configuration options) {
        String uri =
                Preconditions.checkNotNull(
                        options.get(URI),
                        URI.key() + " must be set for table store " + IDENTIFIER + " catalog");
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();
        options.toMap().forEach(hadoopConfig::set);
        hadoopConfig.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        hadoopConfig.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
        return new HiveCatalog(hadoopConfig);
    }
}
