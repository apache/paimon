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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.assembleVariant;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildVariantSchema;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.castShredded;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link VariantMetadata}. */
public class VariantMetadataTest {

    @Test
    void testEmptyAdoptsFirstBuffer() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1,\"b\":\"x\"}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());

        assertThat(metadata.dictionarySize()).isEqualTo(2);
        assertThat(metadata.get(0)).isEqualTo("a");
        assertThat(metadata.get(1)).isEqualTo("b");
    }

    @Test
    void testAdoptKeepsKeysForIdenticalBytes() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1,\"b\":\"x\"}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());
        metadata.adopt(variant.metadataBuffer());

        assertThat(metadata.dictionarySize()).isEqualTo(2);
        assertThat(metadata.get(0)).isEqualTo("a");
        assertThat(metadata.get(1)).isEqualTo("b");
    }

    @Test
    void testAdoptReplacesKeysForDifferentBytes() {
        GenericVariant first = GenericVariant.fromJson("{\"a\":1}");
        GenericVariant second = GenericVariant.fromJson("{\"c\":1}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(first.metadataBuffer());
        metadata.adopt(second.metadataBuffer());

        assertThat(metadata.dictionarySize()).isEqualTo(1);
        assertThat(metadata.get(0)).isEqualTo("c");
    }

    @Test
    void testKeysOfEmptyDictionary() {
        // Arrays do not use dictionary keys.
        GenericVariant variant = GenericVariant.fromJson("[1,2]");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());

        assertThat(metadata.dictionarySize()).isZero();
    }

    @Test
    void testKeysDecodeLazilyAndMemoize() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1,\"b\":\"x\"}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());

        assertThat(metadata.get(0)).isEqualTo("a");
        assertThat(metadata.get(0)).isEqualTo("a");
        assertThat(metadata.get(1)).isEqualTo("b");
    }

    @Test
    void testOutOfBoundDictionaryIdIsRejected() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());

        assertThatThrownBy(() -> metadata.get(1)).hasMessageContaining("MALFORMED_VARIANT");
        assertThatThrownBy(() -> metadata.get(-1)).hasMessageContaining("MALFORMED_VARIANT");
    }

    @Test
    void testCorruptDictionaryHeaderIsRejectedBeforeAllocation() {
        // Header byte with offsetSize=4, then a dictSize field claiming ~2^31-1 entries in a
        // 6-byte buffer. This must fail with malformedVariant instead of allocating a huge
        // String[] and killing the reader with OutOfMemoryError.
        VariantMetadata metadata = VariantMetadata.empty();
        ByteBuffer corrupt =
                ByteBuffer.wrap(
                                new byte[] {
                                    (byte) 0xC0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F, 0
                                })
                        .order(ByteOrder.LITTLE_ENDIAN);

        metadata.adopt(corrupt);
        assertThatThrownBy(() -> metadata.get(0)).hasMessageContaining("MALFORMED_VARIANT");
    }

    @Test
    void testTruncatedOffsetTableIsRejected() {
        // A valid header claiming one entry, but the buffer ends before the offset table.
        VariantMetadata metadata = VariantMetadata.empty();
        ByteBuffer corrupt = ByteBuffer.wrap(new byte[] {0, 1}).order(ByteOrder.LITTLE_ENDIAN);

        metadata.adopt(corrupt);
        assertThatThrownBy(() -> metadata.get(0)).hasMessageContaining("MALFORMED_VARIANT");
    }

    @Test
    void testAdoptDetectsBackingArrayReuse() {
        // Column vectors reuse their backing arrays across rows and batches. The snapshot must
        // detect the overwritten content; a comparison against the caller's (mutated) view would
        // falsely match and serve stale keys.
        GenericVariant first = GenericVariant.fromJson("{\"a\":1}");
        GenericVariant second = GenericVariant.fromJson("{\"c\":1}");
        ByteBuffer firstMetadata = first.metadataBuffer();
        ByteBuffer secondMetadata = second.metadataBuffer();
        byte[] shared = new byte[firstMetadata.remaining()];
        firstMetadata.duplicate().get(shared);
        VariantMetadata metadata = VariantMetadata.empty();

        ByteBuffer view = ByteBuffer.wrap(shared).order(ByteOrder.LITTLE_ENDIAN);
        metadata.adopt(view);
        assertThat(metadata.get(0)).isEqualTo("a");

        // The vector reuses the same array for the next batch's metadata.
        secondMetadata.duplicate().get(shared);
        metadata.adopt(view);
        assertThat(metadata.get(0)).isEqualTo("c");
    }

    @Test
    void testRebuildWithSharedMetadataMatchesPerRowRebuild() {
        RowType shreddedType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"a"});
        RowType physicalType = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(physicalType);

        // The first two rows have leftover fields that are only present in the `value` binary, so
        // rebuilding them decodes keys from the metadata dictionary.
        String[] jsons = {
            "{\"a\":1,\"leftover\":{\"payload\":\"x\"}}",
            "{\"a\":2,\"leftover\":{\"payload\":\"y\",\"extra\":true}}",
            "{\"a\":3}"
        };

        VariantMetadata sharedMetadata = VariantMetadata.empty();
        for (String json : jsons) {
            InternalRow row = castShredded(GenericVariant.fromJson(json), variantSchema);
            Variant rebuiltWithSharedMetadata = assembleVariant(row, variantSchema, sharedMetadata);
            Variant rebuiltPerRow = assembleVariant(row, variantSchema);

            assertThat(rebuiltWithSharedMetadata.toJson()).isEqualTo(rebuiltPerRow.toJson());
            assertThat(rebuiltWithSharedMetadata.toJson())
                    .isEqualTo(GenericVariant.fromJson(json).toJson());
        }
    }

    @Test
    void testAdoptByteOrderIndependentForDirectBuffer() {
        GenericVariant first = GenericVariant.fromJson("{\"a\":1}");
        GenericVariant second = GenericVariant.fromJson("{\"c\":1}");
        byte[] firstMeta = first.metadata();
        byte[] secondMeta = second.metadata();

        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(ByteBuffer.wrap(firstMeta).order(ByteOrder.LITTLE_ENDIAN));
        assertThat(metadata.get(0)).isEqualTo("a");

        // Next row provides different metadata through a direct BIG_ENDIAN buffer.
        ByteBuffer directSecond = ByteBuffer.allocateDirect(secondMeta.length);
        directSecond.put(secondMeta).position(0); // default BIG_ENDIAN
        metadata.adopt(directSecond);
        assertThat(metadata.get(0)).isEqualTo("c");

        // And a direct buffer carrying the very same bytes must be recognized as unchanged.
        ByteBuffer directFirst = ByteBuffer.allocateDirect(firstMeta.length);
        directFirst.put(firstMeta).position(0);
        metadata.adopt(directFirst);
        assertThat(metadata.get(0)).isEqualTo("a");
    }

    @Test
    void testInvalidMetadataDoesNotPoisonCache() {
        // Warm up the dictionary with a valid metadata.
        GenericVariant valid = GenericVariant.fromJson("{\"a\":1}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(valid.metadataBuffer());
        assertThat(metadata.get(0)).isEqualTo("a");

        // A truncated metadata buffer that claims one dictionary entry but has no offset table.
        ByteBuffer corrupt = ByteBuffer.wrap(new byte[] {1, 1}).order(ByteOrder.LITTLE_ENDIAN);

        // The first bad read must be rejected.
        metadata.adopt(corrupt);
        assertThatThrownBy(() -> metadata.get(0)).hasMessageContaining("MALFORMED_VARIANT");

        // Repeated reads of the same invalid buffer must also be rejected, not silently reuse the
        // previously cached dictionary.
        assertThatThrownBy(() -> metadata.get(0)).hasMessageContaining("MALFORMED_VARIANT");

        // After the invalid reads, a new valid metadata must still be adopted correctly.
        GenericVariant another = GenericVariant.fromJson("{\"b\":2}");
        metadata.adopt(another.metadataBuffer());
        assertThat(metadata.dictionarySize()).isEqualTo(1);
        assertThat(metadata.get(0)).isEqualTo("b");
    }

    @Test
    void testRawBufferDoesNotDecode() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1,\"b\":\"x\"}");
        VariantMetadata metadata = VariantMetadata.empty();
        metadata.adopt(variant.metadataBuffer());

        assertThat(metadata.isDecoded()).isFalse();
        ByteBuffer raw = metadata.metadataBuffer();
        assertThat(raw).isEqualTo(variant.metadataBuffer());
        assertThat(metadata.isDecoded()).isFalse();

        // Decoding still happens lazily through the decoded-key cache.
        assertThat(metadata.get(0)).isEqualTo("a");
        assertThat(metadata.isDecoded()).isTrue();
    }
}
