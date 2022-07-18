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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.HiveOrcBatchWrapper;
import org.apache.flink.table.store.format.fs.HadoopReadOnlyFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;

/**
 * A {@link OrcShim} for table store.
 *
 * <p>This is copied from flink-orc except filesystem setting.
 */
public class OrcShimImpl implements OrcShim<VectorizedRowBatch> {

    private static final long serialVersionUID = 1L;

    @Override
    public RecordReader createRecordReader(
            Configuration conf,
            TypeDescription schema,
            int[] selectedFields,
            List<OrcFilters.Predicate> conjunctPredicates,
            org.apache.flink.core.fs.Path path,
            long splitStart,
            long splitLength)
            throws IOException {
        Reader orcReader = createReader(conf, path);

        // get offset and length for the stripes that start in the split
        Tuple2<Long, Long> offsetAndLength =
                getOffsetAndLengthForSplit(splitStart, splitLength, orcReader.getStripes());

        // create ORC row reader configuration
        Reader.Options options =
                new Reader.Options()
                        .schema(schema)
                        .range(offsetAndLength.f0, offsetAndLength.f1)
                        .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
                        .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
                        .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));

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

        // configure selected fields
        options.include(computeProjectionMask(schema, selectedFields));

        // create ORC row reader
        RecordReader orcRowsReader = orcReader.rows(options);

        // assign ids
        schema.getId();

        return orcRowsReader;
    }

    @Override
    public HiveOrcBatchWrapper createBatchWrapper(TypeDescription schema, int batchSize) {
        return new HiveOrcBatchWrapper(schema.createRowBatch(batchSize));
    }

    @Override
    public boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch) throws IOException {
        return reader.nextBatch(rowBatch);
    }

    private static Tuple2<Long, Long> getOffsetAndLengthForSplit(
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
            return Tuple2.of(readStart, readEnd - readStart);
        } else {
            return Tuple2.of(0L, 0L);
        }
    }

    /**
     * Computes the ORC projection mask of the fields to include from the selected
     * fields.rowOrcInputFormat.nextRecord(null).
     *
     * @return The ORC projection mask.
     */
    private static boolean[] computeProjectionMask(TypeDescription schema, int[] selectedFields) {
        // mask with all fields of the schema
        boolean[] projectionMask = new boolean[schema.getMaximumId() + 1];
        // for each selected field
        for (int inIdx : selectedFields) {
            // set all nested fields of a selected field to true
            TypeDescription fieldSchema = schema.getChildren().get(inIdx);
            for (int i = fieldSchema.getId(); i <= fieldSchema.getMaximumId(); i++) {
                projectionMask[i] = true;
            }
        }
        return projectionMask;
    }

    public static Reader createReader(Configuration conf, org.apache.flink.core.fs.Path path)
            throws IOException {
        // open ORC file and create reader
        Path hPath = new Path(path.toUri());

        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);

        // configure filesystem from Flink filesystem
        readerOptions.filesystem(new HadoopReadOnlyFileSystem(path.getFileSystem()));

        return OrcFile.createReader(hPath, readerOptions);
    }
}
