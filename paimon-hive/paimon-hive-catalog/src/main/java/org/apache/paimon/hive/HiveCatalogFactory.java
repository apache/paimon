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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.paimon.hive.HiveCatalogOptions.HADOOP_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.HIVE_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.IDENTIFIER;

/** Factory to create {@link HiveCatalog}. */
public class HiveCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogFactory.class);

    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
                    .stringType()
                    .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
                    .withDescription(
                            "Class name of Hive metastore client.\n"
                                    + "NOTE: This class must directly implements "
                                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(CatalogContext context) {
        HiveConf hiveConf = createHiveConf(context);
        String warehouseStr = context.options().get(CatalogOptions.WAREHOUSE);
        if (warehouseStr == null) {
            warehouseStr =
                    hiveConf.get(METASTOREWAREHOUSE.varname, METASTOREWAREHOUSE.defaultStrVal);
        }
        Path warehouse = new Path(warehouseStr);
        Path uri =
                warehouse.toUri().getScheme() == null
                        ? new Path(FileSystem.getDefaultUri(hiveConf))
                        : warehouse;
        FileIO fileIO;
        try {
            fileIO = FileIO.get(uri, context);
            fileIO.checkOrMkdirs(warehouse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new HiveCatalog(
                fileIO,
                hiveConf,
                context.options().get(METASTORE_CLIENT_CLASS),
                context.options(),
                warehouse.toUri().toString());
    }

    private static HiveConf createHiveConf(CatalogContext context) {
        String uri = context.options().get(CatalogOptions.URI);
        String hiveConfDir = context.options().get(HIVE_CONF_DIR);
        String hadoopConfDir = context.options().get(HADOOP_CONF_DIR);
        HiveConf hiveConf =
                HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir, context.hadoopConf());

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        if (uri != null) {
            hiveConf.set(ConfVars.METASTOREURIS.varname, uri);
        }

        if (hiveConf.get(ConfVars.METASTOREURIS.varname) == null) {
            LOG.error(
                    "Can't find hive metastore uri to connect: "
                            + " either set "
                            + CatalogOptions.URI.key()
                            + " for paimon "
                            + IDENTIFIER
                            + " catalog or set hive.metastore.uris in hive-site.xml or hadoop configurations."
                            + " Will use empty metastore uris, which means we may use a embedded metastore. The may cause unpredictable consensus problem.");
        }

        return hiveConf;
    }
}
