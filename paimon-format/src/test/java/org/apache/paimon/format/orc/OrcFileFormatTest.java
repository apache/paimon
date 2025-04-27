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

import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.format.orc.OrcFileFormatFactory.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrcFileFormatFactory}. */
public class OrcFileFormatTest {

    @Test
    public void testAbsent() {
        Options options = new Options();
        options.setString("orc.haha", "1");
        OrcFileFormat orc =
                new OrcFileFormatFactory().create(new FormatContext(options, 1024, 1024));
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".haha", "")).isEqualTo("1");
    }

    @Test
    public void testPresent() {
        Options options = new Options();
        options.setString("orc.haha", "1");
        options.setString("orc.compress", "zlib");
        OrcFileFormat orc =
                new OrcFileFormatFactory().create(new FormatContext(options, 1024, 1024));
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".haha", "")).isEqualTo("1");
        assertThat(orc.orcProperties().getProperty(IDENTIFIER + ".compress", "")).isEqualTo("zlib");
    }

    @Test
    public void testSupportedDataTypes() {
        OrcFileFormat orc =
                new OrcFileFormatFactory().create(new FormatContext(new Options(), 1024, 1024));

        int index = 0;
        List<DataField> dataFields = new ArrayList<DataField>();
        dataFields.add(new DataField(index++, "boolean_type", DataTypes.BOOLEAN()));
        dataFields.add(new DataField(index++, "tinyint_type", DataTypes.TINYINT()));
        dataFields.add(new DataField(index++, "smallint_type", DataTypes.SMALLINT()));
        dataFields.add(new DataField(index++, "int_type", DataTypes.INT()));
        dataFields.add(new DataField(index++, "bigint_type", DataTypes.BIGINT()));
        dataFields.add(new DataField(index++, "float_type", DataTypes.FLOAT()));
        dataFields.add(new DataField(index++, "double_type", DataTypes.DOUBLE()));
        dataFields.add(new DataField(index++, "char_type", DataTypes.CHAR(10)));
        dataFields.add(new DataField(index++, "varchar_type", DataTypes.VARCHAR(20)));
        dataFields.add(new DataField(index++, "binary_type", DataTypes.BINARY(20)));
        dataFields.add(new DataField(index++, "varbinary_type", DataTypes.VARBINARY(20)));
        dataFields.add(new DataField(index++, "timestamp_type", DataTypes.TIMESTAMP(3)));
        dataFields.add(
                new DataField(
                        index++,
                        "timestamp_ltz_type",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)));
        dataFields.add(new DataField(index++, "date_type", DataTypes.DATE()));
        dataFields.add(new DataField(index++, "decimal_type", DataTypes.DECIMAL(10, 3)));
        orc.validateDataFields(new RowType(dataFields));
    }
}
