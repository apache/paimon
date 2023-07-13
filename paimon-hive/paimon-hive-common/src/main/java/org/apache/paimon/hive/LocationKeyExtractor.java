/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.Warehouse.getDnsPath;

/**
 * declaring the name of the key in the parameters of the Hive metastore table, which indicates
 * where the Paimon table is stored.
 */
public class LocationKeyExtractor {

    // special at the tbproperties with the name paimon_location.
    public static final String TBPROPERTIES_LOCATION_KEY = "paimon_location";

    public static final String INTERNAL_LOCATION = "paimon.internal.location";

    /** Get the real path of Paimon table. */
    public static String getPaimonLocation(@Nullable Configuration conf, Properties properties) {
        // read from table properties
        // if users set HiveCatalogOptions#LOCATION_IN_PROPERTIES
        String location = properties.getProperty(TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        // read what metastore tells us
        location = properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location != null) {
            if (conf != null) {
                try {
                    return getDnsPath(new Path(location), conf).toString();
                } catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return location;
            }
        }

        // for some Hive compatible systems
        if (conf != null) {
            return conf.get("table.original.path");
        }

        return null;
    }

    /** Get the real path of Paimon table. */
    public static String getPaimonLocation(Configuration conf, Table table) throws MetaException {
        // read from table properties
        // if users set HiveCatalogOptions#LOCATION_IN_PROPERTIES
        Map<String, String> params = table.getParameters();
        if (params != null) {
            String location = params.get(TBPROPERTIES_LOCATION_KEY);
            if (location != null) {
                return location;
            }
        }

        // read what metastore tells us
        String location = table.getSd().getLocation();
        if (location != null) {
            location = getDnsPath(new Path(location), conf).toString();
            table.getSd().setLocation(location);
        }
        return location;
    }

    /** Get the real path of Paimon table. */
    public static String getPaimonLocation(JobConf conf) {
        // read what PaimonStorageHandler tells us
        String location = conf.get(INTERNAL_LOCATION);
        if (location != null) {
            return location;
        }

        // read from table properties
        // if users set HiveCatalogOptions#LOCATION_IN_PROPERTIES
        location = conf.get(TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        // for some Hive compatible systems
        location = conf.get("table.original.path");
        if (location != null) {
            return location;
        }

        // read the input dir of this Hive job
        //
        // it is possible that input dir is the directory of a partition,
        // so we should find the root of table by checking if,
        // in each parent directory, the schema directory exists
        location = conf.get(FileInputFormat.INPUT_DIR);
        if (location != null) {
            Path path = new Path(location);
            try {
                FileSystem fs = path.getFileSystem(conf);
                while (path != null) {
                    if (fs.exists(new Path(path, "schema"))) {
                        break;
                    }
                    path = path.getParent();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (path != null) {
                try {
                    return getDnsPath(path, conf).toString();
                } catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return null;
    }

    /**
     * Get the path stated in metastore. It is not necessary the real path of Paimon table.
     *
     * <p>If the path of the Paimon table is moved from the location of the Hive table to properties
     * (see HiveCatalogOptions#LOCATION_IN_PROPERTIES), Hive will add a location for this table
     * based on the warehouse, database, and table automatically. When querying by Hive, an
     * exception may occur because the specified path for split for Paimon may not match the
     * location of Hive. To work around this problem, we specify the path for split as the location
     * of Hive.
     */
    public static String getMetastoreLocation(Configuration conf, List<String> partitionKeys) {
        // read what metastore tells us
        String location = conf.get(hive_metastoreConstants.META_TABLE_LOCATION);
        if (location != null) {
            try {
                return getDnsPath(new Path(location), conf).toString();
            } catch (MetaException e) {
                throw new RuntimeException(e);
            }
        }

        // for some Hive compatible systems
        location = conf.get("table.original.path");
        if (location != null) {
            return location;
        }

        // read the input dir of this Hive job
        //
        // it is possible that input dir is the directory of a partition,
        // so we should find the root of table by removing the partition directory
        location = conf.get(FileInputFormat.INPUT_DIR);
        if (location != null) {
            Path path = new Path(location);
            int numPartitionKeys = partitionKeys.size();
            if (numPartitionKeys > 0
                    && path.getName().startsWith(partitionKeys.get(numPartitionKeys - 1) + "=")) {
                for (int i = 0; i < numPartitionKeys; i++) {
                    path = path.getParent();
                }
            }
            if (path != null) {
                try {
                    return getDnsPath(path, conf).toString();
                } catch (MetaException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return null;
    }
}
