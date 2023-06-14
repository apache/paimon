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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

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

    public static String getLocation(Properties properties) {
        String storageLocation =
                properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
        String propertiesLocation = properties.getProperty(TBPROPERTIES_LOCATION_KEY);
        return propertiesLocation != null ? propertiesLocation : storageLocation;
    }

    public static String getLocation(Table table) {
        String sdLocation = table.getSd().getLocation();

        Map<String, String> params = table.getParameters();

        String propertiesLocation = null;
        if (params != null) {
            propertiesLocation = params.get(TBPROPERTIES_LOCATION_KEY);
        }

        return propertiesLocation != null ? propertiesLocation : sdLocation;
    }

    public static String getLocation(Table table, Configuration conf) throws MetaException {
        String sdLocation = table.getSd().getLocation();
        if (sdLocation != null) {
            org.apache.hadoop.fs.Path path;
            path = getDnsPath(new org.apache.hadoop.fs.Path(sdLocation), conf);
            sdLocation = path.toUri().toString();
            table.getSd().setLocation(sdLocation);
        }

        Map<String, String> params = table.getParameters();

        String propertiesLocation = null;
        if (params != null) {
            propertiesLocation = params.get(TBPROPERTIES_LOCATION_KEY);
        }

        return propertiesLocation != null ? propertiesLocation : sdLocation;
    }
}
