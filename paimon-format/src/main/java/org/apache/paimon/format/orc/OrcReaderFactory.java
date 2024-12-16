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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.format.fs.HadoopReadOnlyFileSystem;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Pool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.format.orc.OrcTypeUtil.convertToOrcSchema;
import static org.apache.paimon.format.orc.reader.AbstractOrcColumnVector.createPaimonVector;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** An ORC reader that produces a stream of {@link ColumnarRow} records. */
public class OrcReaderFactory implements FormatReaderFactory {

    protected final Configuration hadoopConfig;
    protected final TypeDescription schema;
    protected final RowType tableType;
    protected final List<OrcFilters.Predicate> conjunctPredicates;
    protected final int batchSize;
    protected final boolean deletionVectorsEnabled;

    /**
     * @param hadoopConfig the hadoop config for orc reader.
     * @param conjunctPredicates the filter predicates that can be evaluated.
     * @param batchSize the batch size of orc reader.
     */
    public OrcReaderFactory(
            final org.apache.hadoop.conf.Configuration hadoopConfig,
            final RowType readType,
            final List<OrcFilters.Predicate> conjunctPredicates,
            final int batchSize,
            final boolean deletionVectorsEnabled) {
        this.hadoopConfig = checkNotNull(hadoopConfig);
        this.schema = convertToOrcSchema(readType);
        this.tableType = readType;
        this.conjunctPredicates = checkNotNull(conjunctPredicates);
        this.batchSize = batchSize;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
    }

    // ------------------------------------------------------------------------

    @Override
    public OrcVectorizedReader createReader(FormatReaderFactory.Context context)
            throws IOException {
        int poolSize =
                context instanceof OrcFormatReaderContext
                        ? ((OrcFormatReaderContext) context).poolSize()
                        : 1;
        Pool<OrcReaderBatch> poolOfBatches = createPoolOfBatches(context.filePath(), poolSize);

        RecordReader orcReader =
                createRecordReader(
                        hadoopConfig,
                        schema,
                        conjunctPredicates,
                        context.fileIO(),
                        context.filePath(),
                        0,
                        context.fileSize(),
                        context.fileIndex(),
                        deletionVectorsEnabled);
        return new OrcVectorizedReader(orcReader, poolOfBatches);
    }

    /**
     * Creates the {@link OrcReaderBatch} structure, which is responsible for holding the data
     * structures that hold the batch data (column vectors, row arrays, ...) and the batch
     * conversion from the ORC representation to the result format.
     */
    public OrcReaderBatch createReaderBatch(
            Path filePath, VectorizedRowBatch orcBatch, Pool.Recycler<OrcReaderBatch> recycler) {
        List<String> tableFieldNames = tableType.getFieldNames();
        List<DataType> tableFieldTypes = tableType.getFieldTypes();

        // create and initialize the row batch
        ColumnVector[] vectors = new ColumnVector[tableType.getFieldCount()];
        for (int i = 0; i < vectors.length; i++) {
            String name = tableFieldNames.get(i);
            DataType type = tableFieldTypes.get(i);
            vectors[i] =
                    createPaimonVector(
                            orcBatch.cols[tableFieldNames.indexOf(name)], orcBatch, type);
        }
        return new OrcReaderBatch(filePath, orcBatch, new VectorizedColumnBatch(vectors), recycler);
    }

    // ------------------------------------------------------------------------

    private Pool<OrcReaderBatch> createPoolOfBatches(Path filePath, int numBatches) {
        final Pool<OrcReaderBatch> pool = new Pool<>(numBatches);

        for (int i = 0; i < numBatches; i++) {
            final VectorizedRowBatch orcBatch = createBatchWrapper(schema, batchSize / numBatches);
            final OrcReaderBatch batch = createReaderBatch(filePath, orcBatch, pool.recycler());
            pool.add(batch);
        }

        return pool;
    }

    // ------------------------------------------------------------------------

    private static class OrcReaderBatch {

        private final VectorizedRowBatch orcVectorizedRowBatch;
        private final Pool.Recycler<OrcReaderBatch> recycler;

        private final VectorizedColumnBatch paimonColumnBatch;
        private final ColumnarRowIterator result;

        protected OrcReaderBatch(
                final Path filePath,
                final VectorizedRowBatch orcVectorizedRowBatch,
                final VectorizedColumnBatch paimonColumnBatch,
                final Pool.Recycler<OrcReaderBatch> recycler) {
            this.orcVectorizedRowBatch = checkNotNull(orcVectorizedRowBatch);
            this.recycler = checkNotNull(recycler);
            this.paimonColumnBatch = paimonColumnBatch;
            this.result =
                    new ColumnarRowIterator(
                            filePath, new ColumnarRow(paimonColumnBatch), this::recycle);
        }

        /**
         * Puts this batch back into the pool. This should be called after all records from the
         * batch have been returned, typically in the {@link RecordIterator#releaseBatch()} method.
         */
        public void recycle() {
            recycler.recycle(this);
        }

        /** Gets the ORC VectorizedRowBatch structure from this batch. */
        public VectorizedRowBatch orcVectorizedRowBatch() {
            return orcVectorizedRowBatch;
        }

        private ColumnarRowIterator convertAndGetIterator(
                VectorizedRowBatch orcBatch, long rowNumber) {
            // no copying from the ORC column vectors to the Paimon columns vectors necessary,
            // because they point to the same data arrays internally design
            paimonColumnBatch.setNumRows(orcBatch.size);
            result.reset(rowNumber);
            return result;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A vectorized ORC reader. This reader reads an ORC Batch at a time and converts it to one or
     * more records to be returned. An ORC Row-wise reader would convert the batch into a set of
     * rows, while a reader for a vectorized query processor might return the whole batch as one
     * record.
     *
     * <p>The conversion of the {@code VectorizedRowBatch} happens in the specific {@link
     * OrcReaderBatch} implementation.
     *
     * <p>The reader tracks its current position using ORC's <i>row numbers</i>. Each record in a
     * batch is addressed by the starting row number of the batch, plus the number of records to be
     * skipped before.
     */
    private static final class OrcVectorizedReader implements FileRecordReader<InternalRow> {

        private final RecordReader orcReader;
        private final Pool<OrcReaderBatch> pool;

        private OrcVectorizedReader(final RecordReader orcReader, final Pool<OrcReaderBatch> pool) {
            this.orcReader = checkNotNull(orcReader, "orcReader");
            this.pool = checkNotNull(pool, "pool");
        }

        @Nullable
        @Override
        public ColumnarRowIterator readBatch() throws IOException {
            final OrcReaderBatch batch = getCachedEntry();
            final VectorizedRowBatch orcVectorBatch = batch.orcVectorizedRowBatch();

            long rowNumber = orcReader.getRowNumber();
            if (!nextBatch(orcReader, orcVectorBatch)) {
                batch.recycle();
                return null;
            }

            return batch.convertAndGetIterator(orcVectorBatch, rowNumber);
        }

        @Override
        public void close() throws IOException {
            orcReader.close();
        }

        private OrcReaderBatch getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }
    }

    private static RecordReader createRecordReader(
            org.apache.hadoop.conf.Configuration conf,
            TypeDescription schema,
            List<OrcFilters.Predicate> conjunctPredicates,
            FileIO fileIO,
            org.apache.paimon.fs.Path path,
            long splitStart,
            long splitLength,
            @Nullable FileIndexResult fileIndexResult,
            boolean deletionVectorsEnabled)
            throws IOException {
        org.apache.orc.Reader orcReader = createReader(conf, fileIO, path, fileIndexResult);
        try {
            // get offset and length for the stripes that start in the split
            Pair<Long, Long> offsetAndLength =
                    getOffsetAndLengthForSplit(splitStart, splitLength, orcReader.getStripes());

            // create ORC row reader configuration
            org.apache.orc.Reader.Options options =
                    new org.apache.orc.Reader.Options()
                            .schema(schema)
                            .range(offsetAndLength.getLeft(), offsetAndLength.getRight())
                            .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
                            .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
                            .tolerateMissingSchema(
                                    OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));
            if (!conjunctPredicates.isEmpty()
                    && !deletionVectorsEnabled
                    && !(fileIndexResult instanceof BitmapIndexResult)) {
                // row group filter push down will make row number change incorrect
                // so deletion vectors mode and bitmap index cannot work with row group push down
                options.useSelected(OrcConf.READER_USE_SELECTED.getBoolean(conf));
                options.allowSARGToFilter(OrcConf.ALLOW_SARG_TO_FILTER.getBoolean(conf));
            }
            // configure filters
            if (!conjunctPredicates.isEmpty()) {
                SearchArgument.Builder b = SearchArgumentFactory.newBuilder();
                b = b.startAnd();
                for (OrcFilters.Predicate predicate : conjunctPredicates) {
                    predicate.add(b);
                }
                b = b.end();
                options.searchArgument(b.build(), new String[] {});
            }

            // create ORC row reader
            RecordReader orcRowsReader = orcReader.rows(options);

            // assign ids
            schema.getId();

            return orcRowsReader;
        } catch (IOException e) {
            // exception happened, we need to close the reader
            IOUtils.closeQuietly(orcReader);
            throw e;
        }
    }

    private static VectorizedRowBatch createBatchWrapper(TypeDescription schema, int batchSize) {
        return schema.createRowBatch(batchSize);
    }

    private static boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch)
            throws IOException {
        return reader.nextBatch(rowBatch);
    }

    private static Pair<Long, Long> getOffsetAndLengthForSplit(
            long splitStart, long splitLength, List<StripeInformation> stripes) {
        long splitEnd = splitStart + splitLength;
        long readStart = Long.MAX_VALUE;
        long readEnd = Long.MIN_VALUE;

        for (StripeInformation s : stripes) {
            if (splitStart <= s.getOffset() && s.getOffset() < splitEnd) {
                // stripe starts in split, so it is included
                readStart = Math.min(readStart, s.getOffset());
                readEnd = Math.max(readEnd, s.getOffset() + s.getLength());
            }
        }

        if (readStart < Long.MAX_VALUE) {
            // at least one split is included
            return Pair.of(readStart, readEnd - readStart);
        } else {
            return Pair.of(0L, 0L);
        }
    }

    public static org.apache.orc.Reader createReader(
            org.apache.hadoop.conf.Configuration conf,
            FileIO fileIO,
            org.apache.paimon.fs.Path path,
            @Nullable FileIndexResult fileIndexResult)
            throws IOException {
        // open ORC file and create reader
        org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(path.toUri());

        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);

        // configure filesystem from Paimon FileIO
        readerOptions.filesystem(new HadoopReadOnlyFileSystem(fileIO));

        return new ReaderImpl(hPath, readerOptions) {
            @Override
            public RecordReader rows(Options options) throws IOException {
                return new RecordReaderImpl(this, options, fileIndexResult);
            }
        };
    }
}
