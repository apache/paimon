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

package org.apache.paimon.format.orc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.format.orc.writer.OrcBulkWriter;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.format.shredding.SupportsShreddingWritePlan;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.impl.PhysicalFsWriter;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.writer.WriterEncryptionVariant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A factory that creates an ORC {@link FormatWriter}. The factory takes a user supplied {@link
 * Vectorizer} implementation to convert the element into an {@link
 * org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 */
public class OrcWriterFactory implements FormatWriterFactory, SupportsShreddingWritePlan {

    private final Vectorizer<InternalRow> vectorizer;
    private final Properties writerProperties;
    private final Map<String, String> confMap;
    private final boolean legacyTimestampLtzType;

    private OrcFile.WriterOptions writerOptions;
    private final int writeBatchSize;
    private final MemorySize writeBatchMemory;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer implementation.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    @VisibleForTesting
    OrcWriterFactory(Vectorizer<InternalRow> vectorizer) {
        this(vectorizer, new Properties(), new Configuration(false), 1024, MemorySize.ZERO, false);
    }

    public OrcWriterFactory(
            Vectorizer<InternalRow> vectorizer,
            Properties writerProperties,
            Configuration configuration,
            int writeBatchSize,
            MemorySize writeBatchMemory,
            boolean legacyTimestampLtzType) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerProperties = checkNotNull(writerProperties);
        this.confMap = new HashMap<>();
        this.legacyTimestampLtzType = legacyTimestampLtzType;

        // Todo: Replace the Map based approach with a better approach
        for (Map.Entry<String, String> entry : configuration) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        this.writeBatchSize = writeBatchSize;
        this.writeBatchMemory = writeBatchMemory;
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        OrcFile.WriterOptions opts = getWriterOptions();
        if (!writerProperties.containsKey(OrcConf.COMPRESS.getAttribute())) {
            opts.compress(CompressionKind.valueOf(compression.toUpperCase()));
        }

        opts.physicalWriter(
                new PhysicalFsWriter(
                        new FSDataOutputStream(out, null) {
                            @Override
                            public void close() throws IOException {
                                // do nothing
                            }
                        },
                        opts,
                        new WriterEncryptionVariant[0]));

        // The path of the Writer is not used to indicate the destination file
        // in this case since we have used a dedicated physical writer to write
        // to the give output stream directly. However, the path would be used as
        // the key of writer in the ORC memory manager, thus we need to make it unique.
        Path unusedPath = new Path(UUID.randomUUID().toString());
        return new OrcBulkWriter(
                vectorizer,
                new WriterImpl(null, unusedPath, opts),
                out,
                writeBatchSize,
                writeBatchMemory);
    }

    @Override
    public FormatWriter createWithShreddingWritePlan(
            PositionOutputStream out, String compression, ShreddingWritePlan writePlan)
            throws IOException {
        RowType refinedType = (RowType) OrcFileFormat.refineDataType(writePlan.physicalRowType());
        Vectorizer<InternalRow> physicalVectorizer =
                new RowDataVectorizer(
                        OrcTypeUtil.convertToOrcSchema(refinedType),
                        refinedType.getFields(),
                        legacyTimestampLtzType);
        return new OrcWriterFactory(
                        physicalVectorizer,
                        writerProperties,
                        configuration(),
                        writeBatchSize,
                        writeBatchMemory,
                        legacyTimestampLtzType)
                .create(out, compression);
    }

    @Override
    public void commitShreddingMetadata(
            FormatWriter writer, ShreddingWritePlan writePlan, String compression) {
        Map<String, Map<String, String>> fieldMetadata = writePlan.fieldMetadata(compression);
        if (fieldMetadata.isEmpty()) {
            return;
        }

        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put(
                FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY,
                FormatMetadataUtils.buildArrowSchemaMetadata(
                        writePlan.physicalRowType(),
                        fieldMetadata,
                        OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY));
        ((SupportsWriterMetadata) writer).addMetadata(metadata);
    }

    @VisibleForTesting
    protected OrcFile.WriterOptions getWriterOptions() {
        if (null == writerOptions) {
            writerOptions = OrcFile.writerOptions(writerProperties, configuration());
            writerOptions.setSchema(this.vectorizer.getSchema());
        }

        return writerOptions;
    }

    private Configuration configuration() {
        Configuration conf = new ThreadLocalClassLoaderConfiguration();
        for (Map.Entry<String, String> entry : confMap.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }
}
