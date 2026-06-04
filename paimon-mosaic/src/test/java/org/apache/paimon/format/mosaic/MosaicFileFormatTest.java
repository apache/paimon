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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MosaicFileFormat} and {@link MosaicFileFormatFactory}. */
class MosaicFileFormatTest {

    @Test
    void testFactoryIdentifier() {
        MosaicFileFormatFactory factory = new MosaicFileFormatFactory();
        assertThat(factory.identifier()).isEqualTo("mosaic");
    }

    @Test
    void testFactoryCreate() {
        MosaicFileFormatFactory factory = new MosaicFileFormatFactory();
        FileFormatFactory.FormatContext context =
                new FileFormatFactory.FormatContext(new Options(), 1024, 1024);
        assertThat(factory.create(context)).isInstanceOf(MosaicFileFormat.class);
    }

    @Test
    void testCreateReaderFactory() {
        MosaicFileFormat format = createFormat();
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        assertThat(readerFactory).isInstanceOf(MosaicReaderFactory.class);
    }

    @Test
    void testCreateWriterFactory() {
        MosaicFileFormat format = createFormat();
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        assertThat(writerFactory).isInstanceOf(MosaicWriterFactory.class);
    }

    @Test
    void testValidateDataFieldsSupported() {
        MosaicFileFormat format = createFormat();
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE(),
                        DataTypes.FLOAT(),
                        DataTypes.BOOLEAN(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.BYTES());
        format.validateDataFields(rowType);
    }

    @Test
    void testValidateDataFieldsMapUnsupported() {
        MosaicFileFormat format = createFormat();
        RowType rowType = DataTypes.ROW(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThatThrownBy(() -> format.validateDataFields(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("MAP");
    }

    @Test
    void testValidateDataFieldsMultisetUnsupported() {
        MosaicFileFormat format = createFormat();
        RowType rowType = DataTypes.ROW(DataTypes.MULTISET(DataTypes.STRING()));
        assertThatThrownBy(() -> format.validateDataFields(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("MULTISET");
    }

    @Test
    void testCreateStatsExtractor() {
        MosaicFileFormat format = createFormat();
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        assertThat(
                        format.createStatsExtractor(
                                rowType,
                                new org.apache.paimon.statistics.SimpleColStatsCollector.Factory[] {
                                    org.apache.paimon.statistics.SimpleColStatsCollector.from(
                                            "full"),
                                    org.apache.paimon.statistics.SimpleColStatsCollector.from(
                                            "full")
                                }))
                .isPresent();
    }

    private static MosaicFileFormat createFormat() {
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }
}
