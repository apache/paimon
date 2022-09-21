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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.ThreadLocalClassLoaderConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.store.format.orc.OrcFileFormatFactory.IDENTIFIER;

/** Orc {@link FileFormat}. The main code is copied from Flink {@code OrcFileFormatFactory}. */
public class OrcFileFormat extends FileFormat {

    private final Properties orcProperties;
    private final org.apache.hadoop.conf.Configuration readerConf;
    private final org.apache.hadoop.conf.Configuration writerConf;

    public OrcFileFormat(Configuration formatOptions) {
        super(org.apache.flink.orc.OrcFileFormatFactory.IDENTIFIER);
        this.orcProperties = getOrcProperties(formatOptions);
        this.readerConf = new org.apache.hadoop.conf.Configuration();
        this.orcProperties.forEach((k, v) -> readerConf.set(k.toString(), v.toString()));

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        this.writerConf = new ThreadLocalClassLoaderConfiguration();
        conf.forEach(entry -> writerConf.set(entry.getKey(), entry.getValue()));
    }

    @VisibleForTesting
    Properties orcProperties() {
        return orcProperties;
    }

    @Override
    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.of(new OrcFileStatsExtractor(type));
    }

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, int[][] projection, List<Predicate> filters) {
        List<OrcFilters.Predicate> orcPredicates = new ArrayList<>();

        if (filters != null) {
            for (Predicate pred : filters) {
                OrcFilters.Predicate orcPred = OrcFilterConverter.toOrcPredicate(pred);
                if (orcPred != null) {
                    orcPredicates.add(orcPred);
                }
            }
        }

        return OrcInputFormatFactory.create(
                readerConf, type, Projection.of(projection).toTopLevelIndexes(), orcPredicates);
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
        LogicalType[] orcTypes = type.getChildren().toArray(new LogicalType[0]);

        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(type);
        Vectorizer<RowData> vectorizer =
                new RowDataVectorizer(typeDescription.toString(), orcTypes);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(orcProperties, writerConf);
        writerOptions.setSchema(vectorizer.getSchema());

        return new OrcBulkWriterFactory<>(vectorizer, writerOptions);
    }

    private static Properties getOrcProperties(ReadableConfig options) {
        Properties orcProperties = new Properties();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> orcProperties.put(IDENTIFIER + "." + k, v));
        return orcProperties;
    }
}
