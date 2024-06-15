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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.HashMap;

/** Helper for Setting Location in Hive Table Properties. */
public final class TBPropertiesLocationHelper implements LocationHelper {

    public TBPropertiesLocationHelper() {}

    @Override
    public void createPathIfRequired(Path path, FileIO fileIO) throws IOException {
        if (!fileIO.exists(path)) {
            fileIO.mkdirs(path);
        }
    }

    @Override
    public void dropPathIfRequired(Path path, FileIO fileIO) throws IOException {
        if (fileIO.exists(path)) {
            fileIO.delete(path, true);
        }
    }

    @Override
    public void specifyTableLocation(Table table, String location) {
        table.getParameters().put(LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY, location);
    }

    @Override
    public String getTableLocation(Table table) {
        String location = table.getParameters().get(LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        return table.getSd().getLocation();
    }

    @Override
    public void specifyDatabaseLocation(Path path, Database database) {
        HashMap<String, String> properties = new HashMap<>();
        if (database.getParameters() != null) {
            properties.putAll(database.getParameters());
        }
        properties.put(LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY, path.toString());
        database.setParameters(properties);
        database.setLocationUri(null);
    }

    @Override
    public String getDatabaseLocation(Database database) {
        String location =
                database.getParameters().get(LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY);
        if (location != null) {
            return location;
        }

        return database.getLocationUri();
    }
}
