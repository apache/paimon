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

/** Helper for Setting Location in Hive Table Storage. */
public final class StorageLocationHelper implements LocationHelper {

    public StorageLocationHelper() {}

    @Override
    public void createPathIfRequired(Path dbPath, FileIO fileIO) {
        // do nothing
    }

    @Override
    public void dropPathIfRequired(Path path, FileIO fileIO) {
        // do nothing
    }

    @Override
    public void specifyTableLocation(Table table, String location) {
        table.getSd().setLocation(location);
    }

    @Override
    public String getTableLocation(Table table) {
        return table.getSd().getLocation();
    }

    @Override
    public void specifyDatabaseLocation(Path path, Database database) {
        database.setLocationUri(path.toString());
    }

    @Override
    public String getDatabaseLocation(Database database) {
        return database.getLocationUri();
    }
}
