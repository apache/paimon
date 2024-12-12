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

package org.apache.paimon.io;

import org.apache.paimon.fs.Path;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Serializable;
import java.util.Objects;

/**
 * Provides paths for table operations based on warehouse, default write locations and read
 * location.
 */
public class TablePathProvider implements Serializable {
    // the same as the warehouse path;
    private final @NotNull Path warehouseRootPath;
    // the data file external path
    private final Path dataFileExternalPath;
    // the database name;
    private final String databaseName;
    // the table name;
    private final String tableName;

    @TestOnly
    public TablePathProvider(Path path) {
        this.warehouseRootPath = path.getParent().getParent();
        this.dataFileExternalPath = null;
        this.databaseName = path.getParent().getName();
        this.tableName = path.getName();
    }

    public TablePathProvider(Path path, Path dataFileExternalPath) {
        this.warehouseRootPath = path.getParent().getParent();
        this.dataFileExternalPath = dataFileExternalPath;
        this.databaseName = path.getParent().getName();
        this.tableName = path.getName();
    }

    public TablePathProvider(
            @NotNull Path warehouseRootPath,
            Path dataFileExternalPath,
            String databaseName,
            String tableName) {
        this.warehouseRootPath = warehouseRootPath;
        this.dataFileExternalPath = dataFileExternalPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getTableWritePathString() {
        return getTableWritePath().toString();
    }

    public Path getTableWritePath() {
        Path location = dataFileExternalPath != null ? dataFileExternalPath : warehouseRootPath;
        return new Path(location, new Path(databaseName + "/" + tableName));
    }

    public Path getReleativeTableWritePath() {
        return new Path(databaseName + "/" + tableName);
    }

    public Path getDataFileExternalPath() {
        return dataFileExternalPath != null ? dataFileExternalPath : warehouseRootPath;
    }

    public String getWarehouseRootPathString() {
        return warehouseRootPath.toString();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "TablePathProvider{"
                + " warehouseRootPath="
                + warehouseRootPath
                + ", dataFileExternalPath="
                + dataFileExternalPath
                + ", databaseName='"
                + databaseName
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TablePathProvider that = (TablePathProvider) o;
        return warehouseRootPath.equals(that.warehouseRootPath)
                && Objects.equals(dataFileExternalPath, that.dataFileExternalPath)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        int result = warehouseRootPath.hashCode();
        result = 31 * result + Objects.hashCode(dataFileExternalPath);
        result = 31 * result + Objects.hashCode(databaseName);
        result = 31 * result + Objects.hashCode(tableName);
        return result;
    }

    public TablePathProvider copy() {
        return new TablePathProvider(
                warehouseRootPath, dataFileExternalPath, databaseName, tableName);
    }
}
