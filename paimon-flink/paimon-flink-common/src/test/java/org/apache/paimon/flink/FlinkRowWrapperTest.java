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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.options.Options;

import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRowWrapper}. */
public class FlinkRowWrapperTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testMissingBlobDescriptorIsNullWhenCheckingEnabled() {
        java.nio.file.Path missing = tempPath.resolve("missing.blob");
        GenericRowData row = descriptorRow(missing, 1);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isTrue();
    }

    @Test
    public void testExistingBlobDescriptorIsReadableWhenCheckingEnabled() throws Exception {
        byte[] bytes = new byte[] {1, 2, 3};
        java.nio.file.Path blobFile = tempPath.resolve("existing.blob");
        Files.write(blobFile, bytes);
        GenericRowData row = descriptorRow(blobFile, bytes.length);

        FlinkRowWrapper wrapper = wrapper(row, true);

        assertThat(wrapper.isNullAt(0)).isFalse();
        assertThat(wrapper.getBlob(0).toData()).isEqualTo(bytes);
    }

    @Test
    public void testMissingBlobDescriptorUsesDefaultBehaviorWithoutChecking() {
        java.nio.file.Path missing = tempPath.resolve("missing.blob");
        GenericRowData row = descriptorRow(missing, 1);

        FlinkRowWrapper wrapper = wrapper(row, false);
        Blob blob = wrapper.getBlob(0);

        assertThat(wrapper.isNullAt(0)).isFalse();
        assertThat(blob).isNotNull();
    }

    private GenericRowData descriptorRow(java.nio.file.Path path, long length) {
        return GenericRowData.of(
                new BlobDescriptor(path.toUri().toString(), 0, length).serialize());
    }

    private FlinkRowWrapper wrapper(GenericRowData row, boolean checkBlobDescriptorExists) {
        return new FlinkRowWrapper(
                row, CatalogContext.create(new Options()), checkBlobDescriptorExists);
    }
}
