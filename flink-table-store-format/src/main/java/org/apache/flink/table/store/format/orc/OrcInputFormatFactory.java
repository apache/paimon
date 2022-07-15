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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.utils.ReflectionUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** Factory to create orc input format for different Flink versions. */
public class OrcInputFormatFactory {

    public static BulkFormat<RowData, FileSourceSplit> create(
            Configuration conf,
            RowType type,
            int[] projection,
            List<OrcFilters.Predicate> orcPredicates) {
        try {
            return createFrom115(conf, type, projection, orcPredicates);
        } catch (ClassNotFoundException e) {
            try {
                return createFrom114(conf, type, projection, orcPredicates);
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static BulkFormat<RowData, FileSourceSplit> createFrom115(
            Configuration conf,
            RowType type,
            int[] projection,
            List<OrcFilters.Predicate> orcPredicates)
            throws ClassNotFoundException {
        Class<?> formatClass = Class.forName("org.apache.flink.orc.OrcColumnarRowInputFormat");
        try {
            return ReflectionUtils.invokeStaticMethod(
                    formatClass,
                    "createPartitionedFormat",
                    new OrcShimImpl(),
                    conf,
                    type,
                    Collections.emptyList(),
                    null,
                    projection,
                    orcPredicates,
                    2048,
                    (Function<RowType, TypeInformation<RowData>>) InternalTypeInfo::of);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static BulkFormat<RowData, FileSourceSplit> createFrom114(
            Configuration conf,
            RowType type,
            int[] projection,
            List<OrcFilters.Predicate> orcPredicates)
            throws ClassNotFoundException {
        Class<?> formatClass = Class.forName("org.apache.flink.orc.OrcColumnarRowFileInputFormat");
        try {
            return ReflectionUtils.invokeStaticMethod(
                    formatClass,
                    "createPartitionedFormat",
                    new OrcShimImpl(),
                    conf,
                    type,
                    Collections.emptyList(),
                    null,
                    projection,
                    orcPredicates,
                    2048);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
