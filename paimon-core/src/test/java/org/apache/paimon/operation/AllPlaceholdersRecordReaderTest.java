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

package org.apache.paimon.operation;

import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AllPlaceholdersRecordReader}. */
public class AllPlaceholdersRecordReaderTest {

    private static final int BLOB_INDEX = 0;
    private static final int ROW_ID_INDEX = 1;
    private static final int SEQUENCE_NUMBER_INDEX = 2;
    private static final long SEQUENCE_NUMBER = 100L;
    private static final RowType READ_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(BLOB_INDEX, "blob_col", DataTypes.BLOB()),
                            new DataField(
                                    ROW_ID_INDEX, SpecialFields.ROW_ID.name(), DataTypes.BIGINT()),
                            new DataField(
                                    SEQUENCE_NUMBER_INDEX,
                                    SpecialFields.SEQUENCE_NUMBER.name(),
                                    DataTypes.BIGINT())));

    @Test
    public void testFullScan() throws Exception {
        AllPlaceholdersRecordReader reader =
                new AllPlaceholdersRecordReader(
                        5L, 4L, null, READ_ROW_TYPE, BLOB_INDEX, SEQUENCE_NUMBER);

        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        assertNextPlaceholder(iterator, 5L, 0L);
        assertNextPlaceholder(iterator, 6L, 1L);
        assertNextPlaceholder(iterator, 7L, 2L);
        assertNextPlaceholder(iterator, 8L, 3L);
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
        assertThat(reader.readBatch()).isNull();
    }

    @Test
    public void testRowRangePushed() throws Exception {
        AllPlaceholdersRecordReader reader =
                new AllPlaceholdersRecordReader(
                        10L,
                        10L,
                        Arrays.asList(new Range(8, 11), new Range(13, 14), new Range(18, 22)),
                        READ_ROW_TYPE,
                        BLOB_INDEX,
                        SEQUENCE_NUMBER);

        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        assertNextPlaceholder(iterator, 10L, 0L);
        assertNextPlaceholder(iterator, 11L, 1L);
        assertNextPlaceholder(iterator, 13L, 3L);
        assertNextPlaceholder(iterator, 14L, 4L);
        assertNextPlaceholder(iterator, 18L, 8L);
        assertNextPlaceholder(iterator, 19L, 9L);
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
        assertThat(reader.readBatch()).isNull();
    }

    private static void assertNextPlaceholder(
            FileRecordIterator<InternalRow> iterator, long rowId, long returnedPosition)
            throws Exception {
        InternalRow row = iterator.next();
        assertThat(row).isNotNull();
        assertThat(row.getBlob(BLOB_INDEX)).isSameAs(BlobPlaceholder.INSTANCE);
        assertThat(row.getLong(ROW_ID_INDEX)).isEqualTo(rowId);
        assertThat(row.getLong(SEQUENCE_NUMBER_INDEX)).isEqualTo(SEQUENCE_NUMBER);
        assertThat(iterator.returnedPosition()).isEqualTo(returnedPosition);
    }
}
