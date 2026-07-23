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

package org.apache.paimon.flink.function;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.DataTable;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for descriptor to presigned URL functions. */
public class DescriptorToPresignedUrlFunctionTest {

    @Test
    public void testTypeInferenceRequiresLiteralTableAndAcceptsInterval() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnvironment.createTemporarySystemFunction(
                "descriptor_to_presigned_url", DescriptorToPresignedUrlFunction.class);

        assertThat(
                        tableEnvironment.explainSql(
                                "SELECT descriptor_to_presigned_url("
                                        + "'default.t', CAST(NULL AS BYTES), 'png', "
                                        + "INTERVAL '1' HOUR)"))
                .contains("Calc");

        assertThatThrownBy(
                        () ->
                                tableEnvironment.explainSql(
                                        "SELECT descriptor_to_presigned_url("
                                                + "source_table, descriptor, 'png', "
                                                + "INTERVAL '1' HOUR) "
                                                + "FROM (VALUES ('default.t', CAST(NULL AS BYTES))) "
                                                + "AS T(source_table, descriptor)"))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseMessage("Literal expected.");
    }

    @Test
    public void testEvalCachesOneTableAndDelegates() throws Exception {
        Catalog catalog = mock(Catalog.class);
        DataTable table = mock(DataTable.class);
        FileIO fileIO = mock(FileIO.class);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/bucket-0/data", 1, 2);
        when(catalog.getTable(new Identifier("default", "t"))).thenReturn(table);
        when(table.location()).thenReturn(tableRoot);
        when(table.fileIO()).thenReturn(fileIO);
        when(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "png", Duration.ofHours(1)))
                .thenReturn("https://example");

        DescriptorToPresignedUrlFunction function =
                new DescriptorToPresignedUrlFunction("paimon", catalog);

        assertThat(function.eval("default.t", descriptor.serialize(), "png", Duration.ofHours(1)))
                .isEqualTo("https://example");
        assertThat(function.eval("default.t", descriptor.serialize(), "png", Duration.ofHours(1)))
                .isEqualTo("https://example");
        verify(catalog, times(1)).getTable(new Identifier("default", "t"));

        assertThatThrownBy(
                        () ->
                                function.eval(
                                        "default.other",
                                        descriptor.serialize(),
                                        "png",
                                        Duration.ofHours(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one source table");
    }

    @Test
    public void testNullAndTrySemantics() throws Exception {
        Catalog catalog = mock(Catalog.class);
        DataTable table = mock(DataTable.class);
        FileIO fileIO = mock(FileIO.class);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data", 0, 1);
        when(catalog.getTable(new Identifier("default", "t"))).thenReturn(table);
        when(table.location()).thenReturn(tableRoot);
        when(table.fileIO()).thenReturn(fileIO);
        when(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "png", Duration.ofSeconds(1)))
                .thenThrow(new IOException("signing failed"));

        DescriptorToPresignedUrlFunction strict =
                new DescriptorToPresignedUrlFunction("paimon", catalog);
        assertThat(strict.eval("default.t", null, "png", Duration.ofSeconds(1))).isNull();
        assertThat(strict.eval("default.t", descriptor.serialize(), null, Duration.ofSeconds(1)))
                .isNull();
        assertThat(strict.eval("default.t", descriptor.serialize(), "png", null)).isNull();
        verify(catalog, never()).getTable(new Identifier("default", "t"));
        assertThatThrownBy(
                        () ->
                                strict.eval(
                                        "default.t",
                                        descriptor.serialize(),
                                        "png",
                                        Duration.ofSeconds(1)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("signing failed");

        TryDescriptorToPresignedUrlFunction tolerant =
                new TryDescriptorToPresignedUrlFunction("paimon", catalog);
        assertThat(tolerant.eval("default.t", descriptor.serialize(), "png", Duration.ofSeconds(1)))
                .isNull();
    }

    @Test
    public void testRegistrationAndDeterminism() {
        assertThat(BuiltInFunctions.FUNCTIONS)
                .containsEntry(
                        "descriptor_to_presigned_url",
                        DescriptorToPresignedUrlFunction.class.getName())
                .containsEntry(
                        "try_descriptor_to_presigned_url",
                        TryDescriptorToPresignedUrlFunction.class.getName());
        assertThat(new DescriptorToPresignedUrlFunction().isDeterministic()).isFalse();
        assertThat(new TryDescriptorToPresignedUrlFunction().isDeterministic()).isFalse();
    }

    @Test
    public void testCatalogQualifiedTableName() throws Exception {
        Catalog catalog = mock(Catalog.class);
        DataTable table = mock(DataTable.class);
        FileIO fileIO = mock(FileIO.class);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data", 0, 1);
        when(catalog.getTable(new Identifier("default", "t"))).thenReturn(table);
        when(table.location()).thenReturn(tableRoot);
        when(table.fileIO()).thenReturn(fileIO);
        when(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "png", Duration.ofSeconds(1)))
                .thenReturn("https://example");

        DescriptorToPresignedUrlFunction function =
                new DescriptorToPresignedUrlFunction("paimon", catalog);
        assertThat(
                        function.eval(
                                "paimon.default.t",
                                descriptor.serialize(),
                                "png",
                                Duration.ofSeconds(1)))
                .isEqualTo("https://example");

        DescriptorToPresignedUrlFunction crossCatalog =
                new DescriptorToPresignedUrlFunction("paimon", catalog);
        assertThatThrownBy(
                        () ->
                                crossCatalog.eval(
                                        "other.default.t",
                                        descriptor.serialize(),
                                        "png",
                                        Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("paimon.database.table");
    }
}
