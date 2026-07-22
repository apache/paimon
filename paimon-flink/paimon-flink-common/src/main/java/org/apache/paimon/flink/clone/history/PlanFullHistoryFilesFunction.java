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

package org.apache.paimon.flink.clone.history;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.clone.FullHistoryClonePlan;
import org.apache.paimon.clone.FullHistoryCopyPlan;
import org.apache.paimon.clone.FullHistoryPayloadFileVisitor;
import org.apache.paimon.clone.FullHistorySourceFingerprint;
import org.apache.paimon.clone.PathMapping;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Streams physical copy tasks without embedding the complete file set in the job graph. */
class PlanFullHistoryFilesFunction extends ProcessFunction<Integer, FullHistoryCopyPlan.FileCopy> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> sourceCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;
    private final List<String> pathMappings;
    private final String expectedFingerprint;
    private final FullHistoryClonePlan clonePlan;

    private transient Catalog sourceCatalog;
    private transient FileStoreTable sourceTable;
    private transient PathMapping pathMapping;

    PlanFullHistoryFilesFunction(
            Map<String, String> sourceCatalogConfig,
            String sourceDatabase,
            String sourceTableName,
            List<String> pathMappings,
            String expectedFingerprint,
            FullHistoryClonePlan clonePlan) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.pathMappings = pathMappings;
        this.expectedFingerprint = expectedFingerprint;
        this.clonePlan = clonePlan;
    }

    /** Do not annotate with {@code @Override} to retain compatibility with Flink 1.18-. */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /** Do not annotate with {@code @Override} to retain compatibility with Flink 2.0+. */
    public void open(Configuration configuration) throws Exception {
        sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        sourceTable =
                (FileStoreTable)
                        sourceCatalog.getTable(Identifier.create(sourceDatabase, sourceTableName));
        pathMapping = PathMapping.parse(pathMappings);
    }

    @Override
    public void processElement(
            Integer ignored,
            ProcessFunction<Integer, FullHistoryCopyPlan.FileCopy>.Context context,
            Collector<FullHistoryCopyPlan.FileCopy> collector)
            throws Exception {
        FileIO sourceFileIO = sourceTable.fileIO();
        FullHistorySourceFingerprint.verify(sourceTable, expectedFingerprint);
        new FullHistoryPayloadFileVisitor(sourceTable)
                .visit(
                        (source, kind, expectedSize, mappingAnchor) ->
                                collector.collect(
                                        createCopy(
                                                sourceFileIO,
                                                source,
                                                kind,
                                                expectedSize,
                                                mappingAnchor)));
        FullHistorySourceFingerprint.verify(sourceTable, expectedFingerprint);
    }

    private FullHistoryCopyPlan.FileCopy createCopy(
            FileIO sourceFileIO,
            Path source,
            FullHistoryCopyPlan.FileKind kind,
            long expectedSize,
            Path mappingAnchor)
            throws IOException {
        Path target =
                new Path(
                        mappingAnchor == null
                                ? pathMapping.rewriteRequired(source.toString())
                                : pathMapping.rewriteRequiredUnder(
                                        source.toString(), mappingAnchor.toString()));
        checkArgument(
                !source.equals(target),
                "Source and target file paths must be different: %s",
                source);
        clonePlan.validatePayloadTarget(target);
        long size = expectedSize < 0 ? sourceFileIO.getFileSize(source) : expectedSize;
        return new FullHistoryCopyPlan.FileCopy(source, target, kind, size);
    }

    @Override
    public void close() throws Exception {
        if (sourceCatalog != null) {
            sourceCatalog.close();
        }
    }
}
