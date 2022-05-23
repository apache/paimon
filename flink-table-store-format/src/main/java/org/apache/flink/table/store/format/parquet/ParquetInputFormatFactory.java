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

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.utils.ReflectionUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

/** Factory to create parquet input format for different Flink versions. */
public class ParquetInputFormatFactory {

    public static BulkFormat<RowData, FileSourceSplit> create(
            Configuration conf,
            RowType producedRowType,
            TypeInformation<RowData> producedTypeInfo,
            boolean isUtcTimestamp) {
        try {
            return createFrom115(conf, producedRowType, producedTypeInfo, isUtcTimestamp);
        } catch (ClassNotFoundException e) {
            try {
                return createFrom114(conf, producedRowType, isUtcTimestamp);
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static BulkFormat<RowData, FileSourceSplit> createFrom115(
            Configuration conf,
            RowType producedRowType,
            TypeInformation<RowData> producedTypeInfo,
            boolean isUtcTimestamp)
            throws ClassNotFoundException {
        Class<?> formatClass =
                Class.forName("org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat");
        try {
            return ReflectionUtils.invokeStaticMethod(
                    formatClass,
                    "createPartitionedFormat",
                    conf,
                    producedRowType,
                    producedTypeInfo,
                    Collections.emptyList(),
                    null,
                    2048,
                    isUtcTimestamp,
                    true);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static BulkFormat<RowData, FileSourceSplit> createFrom114(
            Configuration conf, RowType producedRowType, boolean isUtcTimestamp)
            throws ClassNotFoundException {
        Class<?> formatClass =
                Class.forName("org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat");
        try {
            return ReflectionUtils.invokeStaticMethod(
                    formatClass,
                    "createPartitionedFormat",
                    conf,
                    producedRowType,
                    Collections.emptyList(),
                    null,
                    2048,
                    isUtcTimestamp,
                    true);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
