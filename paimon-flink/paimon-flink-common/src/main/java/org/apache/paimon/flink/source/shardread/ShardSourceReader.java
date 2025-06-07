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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner.filter;

/** The FileStoreSourceReader for shard read. */
public class ShardSourceReader extends FileStoreSourceReader {

    private static final Logger LOG = LoggerFactory.getLogger(ShardSourceReader.class);

    private static final String SPLIT_STATES_FIELD_NAME = "splitStates";
    private static final String SPLIT_CONTEXT_CLASS_PATH =
            "org.apache.flink.connector.base.source.reader.SourceReaderBase$SplitContext";

    private final TableScan tableScan;

    public ShardSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            TableScan tableScan,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit,
            @Nullable NestedProjectedRowData rowData) {
        super(readerContext, tableRead, metrics, ioManager, limit, rowData);
        this.tableScan = tableScan;
    }

    @Override
    public void addSplits(List<FileStoreSourceSplit> splits) {
        LOG.info("Adding split(s) {} to reader : {}", splits, this);

        if (splits.size() != 1) {
            throw new IllegalArgumentException(
                    "This is a bug, when use shard read, the splits.size() must be equal to 1.");
        }

        List<FileStoreSourceSplit> splitsBelongToThisReader =
                new FileStoreSourceSplitGenerator().createSplits(tableScan.plan());

        FileStoreSourceSplit fileStoreSourceSplit = splits.get(0);
        if (fileStoreSourceSplit instanceof FileStoreSourceSplitWithDpp) {
            LOG.info(
                    "Before DynamicPartitionPruning splitsBelongToThisReader is {}.",
                    splitsBelongToThisReader);
            splitsBelongToThisReader =
                    filterSplitsIfDynamicPartitionPruning(
                            fileStoreSourceSplit, splitsBelongToThisReader);
            LOG.info(
                    "After DynamicPartitionPruning splitsBelongToThisReader is {}.",
                    splitsBelongToThisReader);
        }

        if (splitsBelongToThisReader.size() == 0) {
            // if this reader need not read any split, this reader will finish by follow step.
            // 1. This reader call context.sendSplitRequest here.
            // 2. JobManager send signalNoMoreSplits to this reader.
            // 3. This reader will finish.
            context.sendSplitRequest();
            return;
        }

        Map<String, Object> splitStates = getSplitStatesByReflection();
        Constructor<?> constructor = getSplitContextConstructorByReflection();

        splitsBelongToThisReader.forEach(
                s -> {
                    try {
                        splitStates.put(
                                s.splitId(),
                                constructor.newInstance(s.splitId(), initializedState(s)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        this.splitFetcherManager.addSplits(splitsBelongToThisReader);
    }

    private List<FileStoreSourceSplit> filterSplitsIfDynamicPartitionPruning(
            FileStoreSourceSplit splitsFromEnumerator,
            List<FileStoreSourceSplit> splitsBelongToThisReader) {
        List<FileStoreSourceSplit> splitsAfterDynamicPartitionPruning;
        Projection partitionRowProjection =
                ((FileStoreSourceSplitWithDpp) splitsFromEnumerator).getPartitionRowProjection();
        DynamicFilteringData dynamicFilteringData =
                ((FileStoreSourceSplitWithDpp) splitsFromEnumerator).getDynamicFilteringData();
        splitsAfterDynamicPartitionPruning =
                splitsBelongToThisReader.stream()
                        .filter(
                                newSplit ->
                                        filter(
                                                newSplit,
                                                partitionRowProjection,
                                                dynamicFilteringData))
                        .collect(Collectors.toList());
        return splitsAfterDynamicPartitionPruning;
    }

    private Map<String, Object> getSplitStatesByReflection() {
        Map<String, Object> splitStates;
        try {
            splitStates = ReflectionUtils.getPrivateFieldValue(this, SPLIT_STATES_FIELD_NAME);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("The field " + SPLIT_STATES_FIELD_NAME + " not exist.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "The field " + SPLIT_STATES_FIELD_NAME + " cannot be accessed.", e);
        }

        return splitStates;
    }

    private Constructor<?> getSplitContextConstructorByReflection() {
        Constructor<?> constructor;
        try {
            constructor =
                    ReflectionUtils.getPrivateStaticClassConstructor(
                            SPLIT_CONTEXT_CLASS_PATH, String.class, Object.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "The static inner class " + SPLIT_CONTEXT_CLASS_PATH + " cannot be found.", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "The static inner class " + SPLIT_CONTEXT_CLASS_PATH + " not exist.", e);
        }

        return constructor;
    }
}
