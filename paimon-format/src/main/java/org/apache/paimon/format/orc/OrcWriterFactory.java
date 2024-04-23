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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.writer.OrcBulkWriter;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
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

import static org.apache.paimon.format.OrcOptions.ORC_WRITE_BATCH_SIZE;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A factory that creates an ORC {@link FormatWriter}. The factory takes a user supplied {@link
 * Vectorizer} implementation to convert the element into an {@link
 * org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 */
public class OrcWriterFactory implements FormatWriterFactory {

    private final Vectorizer<InternalRow> vectorizer;
    private final Properties writerProperties;
    private final Map<String, String> confMap;

    private OrcFile.WriterOptions writerOptions;
    private final CoreOptions coreOptions;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer implementation.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcWriterFactory(Vectorizer<InternalRow> vectorizer) {
        this(vectorizer, new Configuration());
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcWriterFactory(Vectorizer<InternalRow> vectorizer, Configuration configuration) {
        this(vectorizer, new Properties(), configuration);
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration, ORC
     * writer properties.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     * @param writerProperties Properties that can be used in ORC WriterOptions.
     */
    public OrcWriterFactory(
            Vectorizer<InternalRow> vectorizer,
            Properties writerProperties,
            Configuration configuration) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerProperties = checkNotNull(writerProperties);
        this.confMap = new HashMap<>();

        // Todo: Replace the Map based approach with a better approach
        for (Map.Entry<String, String> entry : configuration) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        coreOptions = new CoreOptions(this.confMap);
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        if (null != compression) {
            writerProperties.setProperty(OrcConf.COMPRESS.getAttribute(), compression);
        }

        OrcFile.WriterOptions opts = getWriterOptions();
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
                coreOptions.toConfiguration().get(ORC_WRITE_BATCH_SIZE));
    }

    @VisibleForTesting
    protected OrcFile.WriterOptions getWriterOptions() {
        if (null == writerOptions) {
            Configuration conf = new ThreadLocalClassLoaderConfiguration();
            for (Map.Entry<String, String> entry : confMap.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }

            writerOptions = OrcFile.writerOptions(writerProperties, conf);
            writerOptions.setSchema(this.vectorizer.getSchema());
        }

        return writerOptions;
    }
}
