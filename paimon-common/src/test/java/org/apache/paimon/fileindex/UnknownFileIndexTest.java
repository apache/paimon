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

package org.apache.paimon.fileindex;

import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for reading containers with unregistered file index types. */
class UnknownFileIndexTest {

    private static final RowType ROW_TYPE = RowType.builder().field("a", DataTypes.INT()).build();
    private static final Predicate FILTER = new PredicateBuilder(ROW_TYPE).equal(0, 1);

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testUnknownIndexRetainsAllRowsAndWarns(int version) throws Exception {
        ByteArrayOutputStream logs = new ByteArrayOutputStream();
        OutputStreamAppender appender =
                OutputStreamAppender.newBuilder()
                        .setName("unknown-file-index-test")
                        .setTarget(logs)
                        .setLayout(PatternLayout.newBuilder().withPattern("%level %msg%n").build())
                        .build();
        Logger logger = (Logger) LogManager.getLogger(FileIndexFormat.Reader.class.getName());
        Level previousLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try (FileIndexPredicate predicate = predicate(version, false, false)) {
            assertThatThrownBy(() -> predicate.evaluate(FILTER))
                    .hasMessage("Can't find file index for type: unknown");
            assertThatThrownBy(() -> predicate.evaluate(FILTER, false))
                    .hasMessage("Can't find file index for type: unknown");
            assertThat(predicate.evaluate(FILTER, true)).isSameAs(REMAIN);
            assertThat(new String(logs.toByteArray(), StandardCharsets.UTF_8))
                    .contains(
                            "WARN",
                            "Ignoring unregistered file index type 'unknown' for column 'a'");
        } finally {
            logger.setLevel(previousLevel);
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testRegisteredIndexStillFilters(int version) throws Exception {
        try (FileIndexPredicate predicate = predicate(version, true, false)) {
            BitmapIndexResult result = (BitmapIndexResult) predicate.evaluate(FILTER, true);
            assertThat(result.get()).isEqualTo(RoaringBitmap32.bitmapOf(0));
            assertThat(
                            predicate
                                    .evaluate(new PredicateBuilder(ROW_TYPE).equal(0, 3), true)
                                    .remain())
                    .isFalse();
            Predicate or = PredicateBuilder.or(FILTER, new PredicateBuilder(ROW_TYPE).equal(0, 2));
            assertThat(((BitmapIndexResult) predicate.evaluate(or, true)).get())
                    .isEqualTo(RoaringBitmap32.bitmapOf(0, 1));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testUnknownEmptyIndexIsIgnoredWhenEnabled(int version) throws Exception {
        try (FileIndexPredicate predicate = predicate(version, false, true)) {
            assertThat(predicate.evaluate(FILTER).remain()).isFalse();
            assertThat(predicate.evaluate(FILTER, true)).isSameAs(REMAIN);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testTopNRetainsAllRows(int version) throws Exception {
        TopN topN =
                new TopN(
                        new FieldRef(0, "a", DataTypes.INT()),
                        SortValue.SortDirection.ASCENDING,
                        SortValue.NullOrdering.NULLS_FIRST,
                        1);
        try (FileIndexPredicate predicate = predicate(version, false, false)) {
            assertThatThrownBy(() -> predicate.evaluateTopN(topN, REMAIN))
                    .hasMessage("Can't find file index for type: unknown");
            assertThat(predicate.evaluateTopN(topN, REMAIN, true)).isSameAs(REMAIN);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testRegisteredIndexErrorsAreNotIgnored(int version) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, version)) {
            writer.writeIndex("a", "bitmap", out -> out.write(42));
            writer.finish();
        }
        try (FileIndexPredicate predicate =
                new FileIndexPredicate(output.toByteArray(), ROW_TYPE)) {
            assertThatThrownBy(() -> predicate.evaluate(FILTER, true))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static FileIndexPredicate predicate(int version, boolean bitmap, boolean empty)
            throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, version)) {
            writer.writeIndex("a", "unknown", empty ? null : out -> out.write(42));
            if (bitmap) {
                FileIndexWriter index =
                        new BitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
                index.writeRecord(1);
                index.writeRecord(2);
                writer.writeIndex("a", "bitmap", index::writeTo);
            }
            writer.finish();
        }
        return new FileIndexPredicate(output.toByteArray(), ROW_TYPE);
    }
}
