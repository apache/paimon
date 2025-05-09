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

package org.apache.paimon.hudi;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.clone.HivePartitionFiles;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.table.catalog.TableOptionProperties;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.configuration.FlinkOptions.TABLE_TYPE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Utils for cloning Hudi tables. Currently, only support COW Hudi tables in Hive metastore, and it
 * will be cloned to unaware-bucket table.
 */
public class HudiCloneUtils {

    public static boolean isHoodieTable(Table hiveTable) {
        return hiveTable
                .getParameters()
                .getOrDefault(TableOptionProperties.SPARK_SOURCE_PROVIDER, "")
                .equalsIgnoreCase("hudi");
    }

    public static List<HivePartitionFiles> listFiles(
            Table hudiHiveTable,
            FileIO fileIO,
            RowType partitionType,
            @Nullable PartitionPredicate partitionPredicate) {
        Map<String, String> options = hudiHiveTable.getParameters();
        checkTableType(options);

        String location = hudiHiveTable.getSd().getLocation();
        FileIndex fileIndex = new FileIndex(location, options, partitionType, partitionPredicate);

        if (fileIndex.isPartitioned()) {
            return fileIndex.getAllFilteredPartitionFiles(fileIO);
        } else {
            return Collections.singletonList(fileIndex.getUnpartitionedFiles(fileIO));
        }
    }

    private static void checkTableType(Map<String, String> conf) {
        String type = Configuration.fromMap(conf).get(TABLE_TYPE);
        checkArgument(
                HoodieTableType.valueOf(type) == HoodieTableType.COPY_ON_WRITE,
                "Only Hudi COW table is supported yet but found %s table.",
                type);
    }
}
