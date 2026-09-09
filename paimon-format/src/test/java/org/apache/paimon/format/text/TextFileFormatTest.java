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

package org.apache.paimon.format.text;

import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TextFileFormat}. */
class TextFileFormatTest {

    private final TextFileFormat format =
            new TextFileFormat(new FormatContext(new Options(), 1024, 1024));

    @Test
    void testValidateDataFields() {
        assertThatCode(() -> format.validateDataFields(RowType.of(DataTypes.STRING())))
                .doesNotThrowAnyException();

        assertInvalidSchema(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        assertInvalidSchema(RowType.of(DataTypes.INT()));
        assertInvalidSchema(RowType.of());
    }

    private void assertInvalidSchema(RowType rowType) {
        assertThatThrownBy(() -> format.validateDataFields(rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Text format only supports a single string column");
    }
}
