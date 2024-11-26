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

package org.apache.paimon.spark.extensions;

import org.apache.paimon.spark.catalyst.plans.logical.PaimonCallArgument;
import org.apache.paimon.spark.catalyst.plans.logical.PaimonCallStatement;
import org.apache.paimon.spark.catalyst.plans.logical.PaimonNamedArgument;
import org.apache.paimon.spark.catalyst.plans.logical.PaimonPositionalArgument;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.parser.extensions.PaimonParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

import scala.Option;
import scala.collection.JavaConverters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PaimonCallStatement} of {@link PaimonSparkSessionExtensions}. */
public class CallStatementParserTest {

    private SparkSession spark = null;
    private ParserInterface parser = null;

    @BeforeEach
    public void startSparkSession() {
        // Stops and clears active session to avoid loading previous non-stopped session.
        Option<SparkSession> optionalSession =
                SparkSession.getActiveSession().orElse(SparkSession::getDefaultSession);
        if (!optionalSession.isEmpty()) {
            optionalSession.get().stop();
        }
        SparkSession.clearActiveSession();
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();
        parser = spark.sessionState().sqlParser();
    }

    @AfterEach
    public void stopSparkSession() {
        if (spark != null) {
            spark.stop();
            spark = null;
            parser = null;
        }
    }

    @Test
    public void testCallWithNamedArguments() throws ParseException {
        PaimonCallStatement callStatement =
                (PaimonCallStatement)
                        parser.parsePlan(
                                "CALL catalog.system.named_args_func(arg1 => 1, arg2 => 'test', arg3 => true)");
        assertThat(JavaConverters.seqAsJavaList(callStatement.name()))
                .isEqualTo(Arrays.asList("catalog", "system", "named_args_func"));
        assertThat(callStatement.args().size()).isEqualTo(3);
        assertArgument(callStatement, 0, "arg1", 1, DataTypes.IntegerType);
        assertArgument(callStatement, 1, "arg2", "test", DataTypes.StringType);
        assertArgument(callStatement, 2, "arg3", true, DataTypes.BooleanType);
    }

    @Test
    public void testCallWithPositionalArguments() throws ParseException {
        PaimonCallStatement callStatement =
                (PaimonCallStatement)
                        parser.parsePlan(
                                "CALL catalog.system.positional_args_func(1, '${spark.sql.extensions}', 2L, true, 3.0D, 4"
                                        + ".0e1,500e-1BD, "
                                        + "TIMESTAMP '2017-02-03T10:37:30.00Z')");
        assertThat(JavaConverters.seqAsJavaList(callStatement.name()))
                .isEqualTo(Arrays.asList("catalog", "system", "positional_args_func"));
        assertThat(callStatement.args().size()).isEqualTo(8);
        assertArgument(callStatement, 0, 1, DataTypes.IntegerType);
        assertArgument(
                callStatement,
                1,
                PaimonSparkSessionExtensions.class.getName(),
                DataTypes.StringType);
        assertArgument(callStatement, 2, 2L, DataTypes.LongType);
        assertArgument(callStatement, 3, true, DataTypes.BooleanType);
        assertArgument(callStatement, 4, 3.0D, DataTypes.DoubleType);
        assertArgument(callStatement, 5, 4.0e1, DataTypes.DoubleType);
        assertArgument(
                callStatement, 6, new BigDecimal("500e-1"), DataTypes.createDecimalType(3, 1));
        assertArgument(
                callStatement,
                7,
                Timestamp.from(Instant.parse("2017-02-03T10:37:30.00Z")),
                DataTypes.TimestampType);
    }

    @Test
    public void testCallWithMixedArguments() throws ParseException {
        PaimonCallStatement callStatement =
                (PaimonCallStatement)
                        parser.parsePlan("CALL catalog.system.mixed_function(arg1 => 1, 'test')");
        assertThat(JavaConverters.seqAsJavaList(callStatement.name()))
                .isEqualTo(Arrays.asList("catalog", "system", "mixed_function"));
        assertThat(callStatement.args().size()).isEqualTo(2);
        assertArgument(callStatement, 0, "arg1", 1, DataTypes.IntegerType);
        assertArgument(callStatement, 1, "test", DataTypes.StringType);
    }

    @Test
    public void testCallWithParseException() {
        assertThatThrownBy(() -> parser.parsePlan("CALL catalog.system func abc"))
                .isInstanceOf(PaimonParseException.class)
                .hasMessageContaining("missing '(' at 'func'");
    }

    private void assertArgument(
            PaimonCallStatement call, int index, Object expectedValue, DataType expectedType) {
        assertArgument(call, index, null, expectedValue, expectedType);
    }

    private void assertArgument(
            PaimonCallStatement callStatement,
            int index,
            String expectedName,
            Object expectedValue,
            DataType expectedType) {
        if (expectedName == null) {
            PaimonCallArgument callArgument = callStatement.args().apply(index);
            assertCast(callArgument, PaimonPositionalArgument.class);
        } else {
            PaimonNamedArgument namedArgument =
                    assertCast(callStatement.args().apply(index), PaimonNamedArgument.class);
            assertThat(namedArgument.name()).isEqualTo(expectedName);
        }
        assertThat(callStatement.args().apply(index).expr())
                .isEqualTo(Literal$.MODULE$.create(expectedValue, expectedType));
    }

    private <T> T assertCast(Object value, Class<T> expectedClass) {
        assertThat(value).isInstanceOf(expectedClass);
        return expectedClass.cast(value);
    }
}
