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

package org.apache.paimon.spark.function;

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.BlobDescriptorUtils;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for descriptor-to-presigned-URL Spark functions. */
class DescriptorToPresignedUrlFunctionTest {

    private static final Path TABLE_ROOT = new Path("oss://bucket/table");
    private static final BlobDescriptor DESCRIPTOR =
            new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 10, 20);

    @Test
    void testBindAndNonDeterministic() {
        StructType inputType =
                new StructType()
                        .add("source_table", DataTypes.StringType)
                        .add("descriptor", DataTypes.BinaryType)
                        .add("extension", DataTypes.StringType)
                        .add("validity", DataTypes.createDayTimeIntervalType());

        BoundFunction strict = new DescriptorToPresignedUrlUnbound(false).bind(inputType);
        BoundFunction lenient = new DescriptorToPresignedUrlUnbound(true).bind(inputType);

        assertThat(strict).isInstanceOf(DescriptorToPresignedUrlFunction.class);
        assertThat(strict.isDeterministic()).isFalse();
        assertThat(lenient.isDeterministic()).isFalse();
        assertThat(strict.inputTypes())
                .containsExactly(
                        DataTypes.StringType,
                        DataTypes.BinaryType,
                        DataTypes.StringType,
                        DataTypes.createDayTimeIntervalType());
        assertThatThrownBy(
                        () ->
                                new DescriptorToPresignedUrlUnbound(false)
                                        .bind(
                                                new StructType()
                                                        .add("source_table", DataTypes.StringType)
                                                        .add("descriptor", DataTypes.BinaryType)
                                                        .add("extension", DataTypes.StringType)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testResolvedFunction() throws IOException {
        ResolvedDescriptorToPresignedUrlFunction function =
                new ResolvedDescriptorToPresignedUrlFunction(
                        new TestingFileIO(), TABLE_ROOT, false);

        assertThat(
                        function.invoke(
                                DESCRIPTOR.serialize(), UTF8String.fromString("png"), 3_000_000L))
                .hasToString("https://example.test/data.png?validity=3&offset=10&length=20");
        assertThat(function.isDeterministic()).isFalse();
    }

    @Test
    void testNullArguments() throws IOException {
        ResolvedDescriptorToPresignedUrlFunction function =
                new ResolvedDescriptorToPresignedUrlFunction(
                        new TestingFileIO(), TABLE_ROOT, false);

        assertThat(function.invoke(null, UTF8String.fromString("png"), 1_000_000L)).isNull();
        assertThat(function.invoke(DESCRIPTOR.serialize(), null, 1_000_000L)).isNull();
    }

    @Test
    void testValidityMustBePositiveWholeSeconds() {
        ResolvedDescriptorToPresignedUrlFunction function =
                new ResolvedDescriptorToPresignedUrlFunction(
                        new TestingFileIO(), TABLE_ROOT, false);

        assertThatThrownBy(
                        () ->
                                function.invoke(
                                        DESCRIPTOR.serialize(),
                                        UTF8String.fromString("png"),
                                        1_000_001L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whole seconds");
        assertThatThrownBy(
                        () ->
                                function.invoke(
                                        DESCRIPTOR.serialize(), UTF8String.fromString("png"), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void testTryReturnsNullOnRowError() throws IOException {
        ResolvedDescriptorToPresignedUrlFunction function =
                new ResolvedDescriptorToPresignedUrlFunction(new TestingFileIO(), TABLE_ROOT, true);

        assertThat(function.invoke(new byte[] {1}, UTF8String.fromString("png"), 1_000_000L))
                .isNull();
        assertThat(
                        function.invoke(
                                DESCRIPTOR.serialize(), UTF8String.fromString("png"), 1_000_001L))
                .isNull();
    }

    @Test
    void testRejectDescriptorOutsideTableRoot() {
        ResolvedDescriptorToPresignedUrlFunction function =
                new ResolvedDescriptorToPresignedUrlFunction(
                        new TestingFileIO(), TABLE_ROOT, false);
        BlobDescriptor other = new BlobDescriptor("oss://bucket/other/bucket-0/data.blob", 0, 1);

        assertThatThrownBy(
                        () ->
                                function.invoke(
                                        other.serialize(),
                                        UTF8String.fromString("png"),
                                        1_000_000L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("under table root");
    }

    @Test
    void testFileIOSerialization() throws Exception {
        ResolvedDescriptorToPresignedUrlFunction original =
                new ResolvedDescriptorToPresignedUrlFunction(
                        new TestingFileIO(), TABLE_ROOT, false);

        ResolvedDescriptorToPresignedUrlFunction restored = roundTrip(original);

        assertThat(
                        restored.invoke(
                                DESCRIPTOR.serialize(), UTF8String.fromString("jpg"), 2_000_000L))
                .hasToString("https://example.test/data.jpg?validity=2&offset=10&length=20");
    }

    private ResolvedDescriptorToPresignedUrlFunction roundTrip(
            ResolvedDescriptorToPresignedUrlFunction function) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(function);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ResolvedDescriptorToPresignedUrlFunction) input.readObject();
        }
    }

    private static class TestingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public String createBlobPresignedUrl(
                Path tableRoot, BlobDescriptor descriptor, String extension, Duration validity)
                throws IOException {
            BlobDescriptorUtils.validateTableRoot(tableRoot, descriptor);
            return "https://example.test/data."
                    + extension
                    + "?validity="
                    + validity.getSeconds()
                    + "&offset="
                    + descriptor.offset()
                    + "&length="
                    + descriptor.length();
        }
    }
}
