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

package org.apache.orc.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordReaderImpl}. */
public class RecordReaderImplTest {

    @Test
    public void testNullSafeEqualsWithAllNullStatistics() {
        ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(TypeDescription.createString());
        stats.setNull();

        assertThat(
                        RecordReaderImpl.evaluatePredicate(
                                stats, nullSafeEqualsPredicate("value"), null))
                .isEqualTo(SearchArgument.TruthValue.NO);
        assertThat(RecordReaderImpl.evaluatePredicate(stats, nullSafeEqualsPredicate(null), null))
                .isEqualTo(SearchArgument.TruthValue.YES);
    }

    @Test
    public void testMalformedStripeLength() {
        long fileLength = 1000;
        Path path = new Path("file.orc");

        assertThatCode(
                        () ->
                                RecordReaderImpl.validateStripeInformation(
                                        stripe(3, 10, 20, 30), fileLength, path))
                .doesNotThrowAnyException();

        long[][] cases =
                new long[][] {
                    {0, 0, 0, 1L << 31},
                    {0, 0, 0, 1L << 32},
                    {0, 0, 0, -1L},
                    {-1L, 0, 0, 0},
                    {0, -1L, 0, 0},
                    {0, 0, -1L, 0},
                    {1L << 62, 0, 1L << 62, 0},
                    {900, 0, 0, 200}
                };
        for (long[] values : cases) {
            assertThatThrownBy(
                            () ->
                                    RecordReaderImpl.validateStripeInformation(
                                            stripe(values[0], values[1], values[2], values[3]),
                                            fileLength,
                                            path))
                    .isInstanceOf(FileFormatException.class)
                    .hasMessageContaining("Malformed ORC file")
                    .hasMessageContaining("Invalid stripe offset/length");
        }
    }

    private static StripeInformation stripe(
            long offset, long indexLength, long dataLength, long footerLength) {
        return new ReaderImpl.StripeInformationImpl(
                OrcProto.StripeInformation.newBuilder()
                        .setOffset(offset)
                        .setIndexLength(indexLength)
                        .setDataLength(dataLength)
                        .setFooterLength(footerLength)
                        .setNumberOfRows(0)
                        .build(),
                0,
                0,
                null);
    }

    private static PredicateLeaf nullSafeEqualsPredicate(Object literal) {
        return new PredicateLeaf() {
            @Override
            public Operator getOperator() {
                return Operator.NULL_SAFE_EQUALS;
            }

            @Override
            public Type getType() {
                return Type.STRING;
            }

            @Override
            public String getColumnName() {
                return "field";
            }

            @Override
            public Object getLiteral() {
                return literal;
            }

            @Override
            public List<Object> getLiteralList() {
                return Collections.emptyList();
            }

            @Override
            public int getId() {
                return 0;
            }
        };
    }
}
