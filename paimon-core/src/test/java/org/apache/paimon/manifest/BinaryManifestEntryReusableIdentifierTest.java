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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryManifestEntry.ReusableIdentifier;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReusableIdentifier}. */
class BinaryManifestEntryReusableIdentifierTest {

    @Test
    void testEncodesIdentifierFields() {
        ReusableIdentifier identifier = new ReusableIdentifier();

        assertThat(
                        identifier.replace(
                                entry(3, 2, "f", new String[] {"a", "bc"}, new byte[] {7, 8}, "x")))
                .isSameAs(identifier);

        assertThat(Arrays.copyOf(identifier.bytes(), identifier.length()))
                .containsExactly(
                        0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 1, 102, 0, 0, 0, 2, 0, 0, 0, 1, 97, 0, 0,
                        0, 2, 98, 99, 0, 0, 0, 2, 7, 8, 0, 0, 0, 1, 120);
    }

    @Test
    void testReusesAndReleasesBuffer() {
        ReusableIdentifier identifier = new ReusableIdentifier();
        BinaryManifestEntry entry = entry(1, 0, "file", new String[0], null, null);

        identifier.replace(entry);
        byte[] expected = Arrays.copyOf(identifier.bytes(), identifier.length());
        assertThat(expected).endsWith((byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff);

        identifier.release();
        assertThat(identifier.length()).isZero();
        assertThat(identifier.bytes()).isEmpty();

        identifier.replace(entry);
        assertThat(Arrays.copyOf(identifier.bytes(), identifier.length())).isEqualTo(expected);
    }

    @Test
    void testRejectsNullEntry() {
        assertThatThrownBy(() -> new ReusableIdentifier().replace(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeletedIdentifierSetReusesIdentifierForEntryLookup() {
        DeletedIdentifierSet identifiers = new DeletedIdentifierSet();
        BinaryManifestEntry first = entry(1, 0, "first", new String[0], null, null);
        BinaryManifestEntry second = entry(2, 0, "second", new String[0], null, null);

        identifiers.add(first);
        assertThat(identifiers.contains(first)).isTrue();
        assertThat(identifiers.contains(second)).isFalse();
        assertThat(identifiers.contains(first)).isTrue();

        identifiers.add(second);
        assertThat(identifiers.contains(first)).isTrue();
        assertThat(identifiers.contains(second)).isTrue();
    }

    private static BinaryManifestEntry entry(
            int bucket,
            int level,
            String fileName,
            String[] extraFiles,
            @Nullable byte[] embeddedIndex,
            @Nullable String externalPath) {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        RowType fileType =
                DataFileMeta.SCHEMA.project(
                        DataFileMeta.LEVEL,
                        DataFileMeta.FILE_NAME,
                        DataFileMeta.EXTRA_FILES,
                        DataFileMeta.EMBEDDED_FILE_INDEX,
                        DataFileMeta.EXTERNAL_PATH);
        List<DataField> fields =
                Arrays.asList(
                        manifestType.getField(ManifestEntry.PARTITION),
                        manifestType.getField(ManifestEntry.BUCKET),
                        manifestType.getField(ManifestEntry.FILE).newType(fileType));
        Object[] extraFileValues = new Object[extraFiles.length];
        for (int i = 0; i < extraFiles.length; i++) {
            extraFileValues[i] = BinaryString.fromString(extraFiles[i]);
        }
        return BinaryManifestEntry.Projection.create(new RowType(false, fields))
                .createEntry()
                .replace(
                        GenericRow.of(
                                serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                bucket,
                                GenericRow.of(
                                        level,
                                        BinaryString.fromString(fileName),
                                        new GenericArray(extraFileValues),
                                        embeddedIndex,
                                        externalPath == null
                                                ? null
                                                : BinaryString.fromString(externalPath))));
    }
}
