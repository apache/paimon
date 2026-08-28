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

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.sst.SstFileUtils.crc32c;

/**
 * Portable V1 container layout for partitioned, demand-paged FM indexes.
 *
 * <p>Each physical index file contains one or more canonical, contiguous partitions followed by a
 * checksummed container directory and fixed footer. A partition contains dense-alphabet blocked
 * quaternary wavelet levels, sampled-SA mask and values, null mask, row-boundary mask,
 * exact-verification value pages, its directory, and a fixed footer. Every independently readable
 * block records its offset, stored and uncompressed lengths, compression ID and CRC32C. The reader
 * validates all physical ranges before allocating decoded buffers and verifies the stored checksum
 * before decompression.
 */
final class FMIndexFile {

    static final int TERMINATOR = 0;
    static final int SEPARATOR = 1;
    static final int FIRST_BYTE_SYMBOL = 2;
    static final int MAX_ALPHABET_SIZE = 258;

    private static final int PARTITION_MAGIC = 0x464D4950;
    private static final int CONTAINER_MAGIC = 0x464D4958;
    private static final int VERSION = 1;
    private static final int INDEX_META_MAGIC = 0x464D4D45;
    private static final int INDEX_META_VERSION = 1;
    private static final int INDEX_META_HEADER_LENGTH = 28;
    private static final int INDEX_META_PARTITION_LENGTH = 28;
    private static final int INDEX_META_CHECKSUM_LENGTH = Integer.BYTES;
    private static final int FEATURE_VALUE_SAMPLED_SA = 1;
    private static final int FEATURE_DENSE_QUAD_WAVELET = 1 << 1;
    private static final int FEATURE_SEPARATOR_ROW_IDS = 1 << 2;
    private static final int FEATURE_EXACT_DENSE_FALLBACK = 1 << 3;
    private static final int FEATURE_FLAGS =
            FEATURE_VALUE_SAMPLED_SA
                    | FEATURE_DENSE_QUAD_WAVELET
                    | FEATURE_SEPARATOR_ROW_IDS
                    | FEATURE_EXACT_DENSE_FALLBACK;

    static final int BLOCK_WORDS = 4096;
    static final int BLOCK_BITS = BLOCK_WORDS * Long.SIZE;
    static final int QUAD_VALUES_PER_WORD = 32;
    static final int QUAD_BLOCK_VALUES = BLOCK_WORDS * QUAD_VALUES_PER_WORD;
    static final int VALUE_BLOCK_INTS = 8192;
    static final int BLOCK_INFO_LENGTH = 24;
    static final int PARTITION_FOOTER_LENGTH = 64;
    static final int CONTAINER_FOOTER_LENGTH = 64;
    static final int FOOTER_CHECKSUM_OFFSET = 60;
    static final int MAX_DIRECTORY_UNCOMPRESSED_LENGTH = 16 * 1024 * 1024;
    static final int MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH = 64 * 1024;
    static final int MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH = 64 * 1024 * 1024;

    private FMIndexFile() {}

    static byte[] writeIndexMeta(long firstRowId, long rowCount, List<PartitionMeta> partitions) {
        Preconditions.checkArgument(firstRowId >= 0 && rowCount > 0, "Invalid FM index row range.");
        Preconditions.checkArgument(
                firstRowId <= Long.MAX_VALUE - rowCount,
                "FM index row range overflows the supported row ID space.");
        Preconditions.checkArgument(!partitions.isEmpty(), "FM index must contain partitions.");
        long encodedLength =
                INDEX_META_HEADER_LENGTH
                        + (long) partitions.size() * INDEX_META_PARTITION_LENGTH
                        + INDEX_META_CHECKSUM_LENGTH;
        Preconditions.checkArgument(
                encodedLength <= MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
                "FM index partition directory exceeds the supported size.");
        byte[] bytes = new byte[(int) encodedLength];
        writeInt(bytes, 0, INDEX_META_MAGIC);
        writeInt(bytes, 4, INDEX_META_VERSION);
        writeLong(bytes, 8, firstRowId);
        writeLong(bytes, 16, rowCount);
        writeInt(bytes, 24, partitions.size());
        int offset = INDEX_META_HEADER_LENGTH;
        for (PartitionMeta partition : partitions) {
            writeLong(bytes, offset, partition.startOffset);
            writeLong(bytes, offset + 8, partition.endOffset);
            writeLong(bytes, offset + 16, partition.firstRowId);
            writeInt(bytes, offset + 24, partition.rowCount);
            offset += INDEX_META_PARTITION_LENGTH;
        }
        writeInt(bytes, offset, indexMetaChecksum(bytes));
        // Validate writer-produced metadata through the same canonical parser used by readers.
        readIndexMeta(bytes);
        return bytes;
    }

    static IndexMeta readIndexMeta(byte[] bytes) {
        Preconditions.checkState(
                bytes.length >= INDEX_META_HEADER_LENGTH + INDEX_META_CHECKSUM_LENGTH
                        && bytes.length <= MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
                "Invalid FM index manifest metadata length.");
        Preconditions.checkState(
                readInt(bytes, 0) == INDEX_META_MAGIC, "Invalid FM index manifest metadata magic.");
        Preconditions.checkState(
                readInt(bytes, 4) == INDEX_META_VERSION,
                "Unsupported FM index manifest metadata version: %s.",
                readInt(bytes, 4));
        int partitionCount = readInt(bytes, 24);
        Preconditions.checkState(partitionCount > 0, "FM index must contain partitions.");
        long expectedLength =
                INDEX_META_HEADER_LENGTH
                        + (long) partitionCount * INDEX_META_PARTITION_LENGTH
                        + INDEX_META_CHECKSUM_LENGTH;
        Preconditions.checkState(
                expectedLength == bytes.length, "Invalid FM index manifest metadata length.");
        Preconditions.checkState(
                readInt(bytes, bytes.length - INDEX_META_CHECKSUM_LENGTH)
                        == indexMetaChecksum(bytes),
                "FM index manifest metadata checksum mismatch.");
        long firstRowId = readLong(bytes, 8);
        long rowCount = readLong(bytes, 16);
        Preconditions.checkState(firstRowId >= 0 && rowCount > 0, "Invalid FM index row range.");
        Preconditions.checkState(
                firstRowId <= Long.MAX_VALUE - rowCount,
                "FM index row range overflows the supported row ID space.");
        List<PartitionMeta> partitions = new ArrayList<>(partitionCount);
        long expectedOffset = 0;
        long expectedRowId = firstRowId;
        int offset = INDEX_META_HEADER_LENGTH;
        for (int i = 0; i < partitionCount; i++) {
            long startOffset = readLong(bytes, offset);
            long endOffset = readLong(bytes, offset + 8);
            long partitionFirstRowId = readLong(bytes, offset + 16);
            int partitionRowCount = readInt(bytes, offset + 24);
            Preconditions.checkState(
                    startOffset == expectedOffset
                            && endOffset > startOffset
                            && endOffset - startOffset >= PARTITION_FOOTER_LENGTH,
                    "FM index partitions are not canonical and contiguous.");
            Preconditions.checkState(
                    partitionFirstRowId == expectedRowId && partitionRowCount > 0,
                    "FM index partition row ranges are not canonical and contiguous.");
            Preconditions.checkState(
                    expectedRowId <= Long.MAX_VALUE - partitionRowCount,
                    "FM index partition row range overflows the supported row ID space.");
            partitions.add(
                    new PartitionMeta(
                            startOffset, endOffset, partitionFirstRowId, partitionRowCount));
            expectedOffset = endOffset;
            expectedRowId += partitionRowCount;
            offset += INDEX_META_PARTITION_LENGTH;
        }
        Preconditions.checkState(
                expectedRowId == firstRowId + rowCount,
                "FM index partition row counts do not match the file row count.");
        return new IndexMeta(firstRowId, rowCount, partitions);
    }

    static BlockInfo writeBlock(
            PositionOutputStream stream,
            DataOutputStream out,
            byte[] uncompressed,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        BlockCompressionType compression = BlockCompressionType.NONE;
        byte[] stored = uncompressed;
        int storedLength = uncompressed.length;
        if (compressionFactory != null && uncompressed.length > 0) {
            BlockCompressor compressor = compressionFactory.getCompressor();
            byte[] compressed = new byte[compressor.getMaxCompressedSize(uncompressed.length)];
            int compressedLength =
                    compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0);
            if (compressedLength < uncompressed.length - (uncompressed.length / 8)) {
                compression = compressionFactory.getCompressionType();
                stored = compressed;
                storedLength = compressedLength;
            }
        }
        long offset = stream.getPos();
        out.write(stored, 0, storedLength);
        int checksum =
                crc32c(new MemorySlice(MemorySegment.wrap(stored), 0, storedLength), compression);
        return new BlockInfo(
                offset, storedLength, uncompressed.length, compression.persistentId(), checksum);
    }

    static QuadVectorMeta writeQuadVector(
            PositionOutputStream stream,
            DataOutputStream out,
            long[] words,
            int valueLength,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        Preconditions.checkArgument(
                words.length == wordsForQuads(valueLength),
                "Invalid FM quaternary-vector word count.");
        List<QuadBlockMeta> blocks = new ArrayList<>();
        int[] totalCounts = new int[4];
        for (int wordStart = 0; wordStart < words.length; wordStart += BLOCK_WORDS) {
            int wordCount = Math.min(BLOCK_WORDS, words.length - wordStart);
            int firstValue = wordStart * QUAD_VALUES_PER_WORD;
            int valueCount = Math.min(valueLength - firstValue, wordCount * QUAD_VALUES_PER_WORD);
            int[] counts = new int[4];
            for (int i = 0; i < wordCount; i++) {
                int validValues =
                        Math.min(QUAD_VALUES_PER_WORD, valueCount - i * QUAD_VALUES_PER_WORD);
                for (int digit = 0; digit < 4; digit++) {
                    counts[digit] += countDigit(words[wordStart + i], digit, validValues);
                }
            }
            BlockInfo block =
                    writeBlock(
                            stream,
                            out,
                            encodeQuadBlock(words, wordStart, wordCount, valueCount),
                            compressionFactory);
            blocks.add(
                    new QuadBlockMeta(
                            firstValue,
                            valueCount,
                            java.util.Arrays.copyOf(totalCounts, 4),
                            counts,
                            block));
            for (int digit = 0; digit < 4; digit++) {
                totalCounts[digit] += counts[digit];
            }
        }
        return new QuadVectorMeta(valueLength, totalCounts, blocks);
    }

    static BitVectorMeta writeBitVector(
            PositionOutputStream stream,
            DataOutputStream out,
            long[] words,
            int bitLength,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        Preconditions.checkArgument(
                words.length == wordsForBits(bitLength), "Invalid FM bit-vector word count.");
        List<BitBlockMeta> blocks = new ArrayList<>();
        int totalOnes = 0;
        for (int wordStart = 0; wordStart < words.length; wordStart += BLOCK_WORDS) {
            int wordCount = Math.min(BLOCK_WORDS, words.length - wordStart);
            int firstBit = wordStart * Long.SIZE;
            int blockBits = Math.min(bitLength - firstBit, wordCount * Long.SIZE);
            byte[] bytes = encodeBitBlock(words, wordStart, wordCount);
            BlockInfo block = writeBlock(stream, out, bytes, compressionFactory);
            int ones = 0;
            for (int i = 0; i < wordCount; i++) {
                ones += Long.bitCount(words[wordStart + i]);
            }
            blocks.add(new BitBlockMeta(firstBit, blockBits, totalOnes, ones, block));
            totalOnes += ones;
        }
        return new BitVectorMeta(bitLength, totalOnes, blocks);
    }

    static IntVectorMeta writeIntVector(
            PositionOutputStream stream,
            DataOutputStream out,
            int[] values,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        List<IntBlockMeta> blocks = new ArrayList<>();
        for (int start = 0; start < values.length; start += VALUE_BLOCK_INTS) {
            int count = Math.min(VALUE_BLOCK_INTS, values.length - start);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(count * Integer.BYTES);
            DataOutputStream data = new DataOutputStream(bytes);
            for (int i = 0; i < count; i++) {
                data.writeInt(values[start + i]);
            }
            data.flush();
            blocks.add(
                    new IntBlockMeta(
                            start,
                            count,
                            writeBlock(stream, out, bytes.toByteArray(), compressionFactory)));
        }
        return new IntVectorMeta(values.length, blocks);
    }

    static BlockInfo writeDirectory(
            PositionOutputStream stream,
            DataOutputStream out,
            Directory directory,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes);
        data.writeInt(directory.rowCount);
        data.writeInt(directory.textLength);
        data.writeInt(directory.sampleRate);
        data.writeInt(directory.levelCount);
        data.writeInt(directory.alphabetSize);
        data.writeInt(BLOCK_WORDS);
        for (int symbol : directory.byteToSymbol) {
            data.writeInt(symbol);
        }
        for (int value : directory.cumulativeCounts) {
            data.writeInt(value);
        }
        for (int level = 0; level < directory.levelCount; level++) {
            for (int start : directory.digitStarts[level]) {
                data.writeInt(start);
            }
            writeQuadVectorMeta(data, directory.wavelets[level]);
        }
        writeBitVectorMeta(data, directory.sampledRows);
        writeIntVectorMeta(data, directory.sampleValues);
        writeBitVectorMeta(data, directory.nullRows);
        writeBitVectorMeta(data, directory.rowBoundaries);
        data.writeInt(directory.verificationPages.size());
        for (VerificationPageMeta page : directory.verificationPages) {
            data.writeInt(page.firstRow);
            data.writeInt(page.rowCount);
            writeBlockInfo(data, page.block);
        }
        data.flush();
        byte[] uncompressed = bytes.toByteArray();
        Preconditions.checkState(
                uncompressed.length <= MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
                "FM index directory exceeds the supported size: %s.",
                uncompressed.length);
        return writeBlock(stream, out, uncompressed, compressionFactory);
    }

    static void writeFooter(
            DataOutputStream out,
            BlockInfo directory,
            long firstRowId,
            int rowCount,
            int textLength,
            int sampleRate)
            throws IOException {
        byte[] bytes = new byte[PARTITION_FOOTER_LENGTH];
        writeBlockInfo(bytes, 0, directory);
        writeLong(bytes, 24, firstRowId);
        writeInt(bytes, 32, rowCount);
        writeInt(bytes, 36, textLength);
        writeInt(bytes, 40, sampleRate);
        writeInt(bytes, 44, FEATURE_FLAGS);
        writeInt(bytes, 52, VERSION);
        writeInt(bytes, 56, PARTITION_MAGIC);
        writeInt(bytes, FOOTER_CHECKSUM_OFFSET, footerChecksum(bytes));
        out.write(bytes);
        out.flush();
    }

    static BlockInfo writeContainerDirectory(
            PositionOutputStream stream,
            DataOutputStream out,
            byte[] indexMeta,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        return writeBlock(stream, out, indexMeta, compressionFactory);
    }

    static void writeContainerFooter(
            DataOutputStream out,
            BlockInfo directory,
            long firstRowId,
            long rowCount,
            int partitionCount)
            throws IOException {
        byte[] bytes = new byte[CONTAINER_FOOTER_LENGTH];
        writeBlockInfo(bytes, 0, directory);
        writeLong(bytes, 24, firstRowId);
        writeLong(bytes, 32, rowCount);
        writeInt(bytes, 40, partitionCount);
        writeInt(bytes, 44, FEATURE_FLAGS);
        writeInt(bytes, 52, VERSION);
        writeInt(bytes, 56, CONTAINER_MAGIC);
        writeInt(bytes, FOOTER_CHECKSUM_OFFSET, footerChecksum(bytes));
        out.write(bytes);
        out.flush();
    }

    static ContainerFooter readContainerFooter(SeekableInputStream input, long fileSize)
            throws IOException {
        Preconditions.checkState(
                fileSize >= CONTAINER_FOOTER_LENGTH,
                "Invalid FM index container size: %s.",
                fileSize);
        byte[] bytes = readAt(input, fileSize - CONTAINER_FOOTER_LENGTH, CONTAINER_FOOTER_LENGTH);
        Preconditions.checkState(
                readInt(bytes, 56) == CONTAINER_MAGIC,
                "File is not an FM index container (bad footer magic).");
        Preconditions.checkState(
                readInt(bytes, 52) == VERSION,
                "Unsupported FM index container version: %s.",
                readInt(bytes, 52));
        int expectedChecksum = readInt(bytes, FOOTER_CHECKSUM_OFFSET);
        int actualChecksum = footerChecksum(bytes);
        Preconditions.checkState(
                expectedChecksum == actualChecksum,
                "FM index container footer checksum mismatch: expected=%s, actual=%s.",
                expectedChecksum,
                actualChecksum);
        Preconditions.checkState(
                readInt(bytes, 44) == FEATURE_FLAGS,
                "Unsupported FM index container feature flags: %s.",
                readInt(bytes, 44));
        Preconditions.checkState(
                readInt(bytes, 48) == 0, "Invalid FM index container reserved field.");

        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        BlockInfo directory = readBlockInfo(data);
        long firstRowId = data.readLong();
        long rowCount = data.readLong();
        int partitionCount = data.readInt();
        Preconditions.checkState(firstRowId >= 0 && rowCount > 0, "Invalid FM index row range.");
        Preconditions.checkState(
                firstRowId <= Long.MAX_VALUE - rowCount,
                "FM index row range overflows the supported row ID space.");
        Preconditions.checkState(
                partitionCount > 0 && partitionCount <= rowCount,
                "Invalid FM index partition count.");
        validateBlock(
                directory,
                fileSize - CONTAINER_FOOTER_LENGTH,
                MAX_DIRECTORY_UNCOMPRESSED_LENGTH,
                false);
        Preconditions.checkState(
                directory.offset + directory.storedLength == fileSize - CONTAINER_FOOTER_LENGTH,
                "FM index container directory is not immediately before the footer.");
        return new ContainerFooter(directory, firstRowId, rowCount, partitionCount);
    }

    static IndexMeta readContainerDirectory(
            SeekableInputStream input, ContainerFooter footer, long fileSize) throws IOException {
        IndexMeta metadata = readIndexMeta(readBlock(input, footer.directory, fileSize));
        Preconditions.checkState(
                metadata.firstRowId == footer.firstRowId
                        && metadata.rowCount == footer.rowCount
                        && metadata.partitions.size() == footer.partitionCount,
                "FM index container footer and directory metadata do not match.");
        Preconditions.checkState(
                metadata.partitions.get(metadata.partitions.size() - 1).endOffset
                        == footer.directory.offset,
                "FM index partitions do not exactly cover the container payload.");
        return metadata;
    }

    static Footer readFooter(SeekableInputStream input, PartitionMeta partition, long fileSize)
            throws IOException {
        Preconditions.checkState(
                partition.startOffset >= 0
                        && partition.endOffset <= fileSize
                        && partition.endOffset - partition.startOffset >= PARTITION_FOOTER_LENGTH,
                "Invalid FM index partition range.");
        long footerOffset = partition.endOffset - PARTITION_FOOTER_LENGTH;
        byte[] bytes = readAt(input, footerOffset, PARTITION_FOOTER_LENGTH);
        Preconditions.checkState(
                readInt(bytes, 56) == PARTITION_MAGIC,
                "File is not an FM index partition (bad footer magic).");
        Preconditions.checkState(
                readInt(bytes, 52) == VERSION,
                "Unsupported FM index partition version: %s.",
                readInt(bytes, 52));
        int expectedChecksum = readInt(bytes, FOOTER_CHECKSUM_OFFSET);
        int actualChecksum = footerChecksum(bytes);
        Preconditions.checkState(
                expectedChecksum == actualChecksum,
                "FM index partition footer checksum mismatch: expected=%s, actual=%s.",
                expectedChecksum,
                actualChecksum);
        Preconditions.checkState(
                readInt(bytes, 44) == FEATURE_FLAGS,
                "Unsupported FM index partition feature flags: %s.",
                readInt(bytes, 44));
        Preconditions.checkState(
                readInt(bytes, 48) == 0, "Invalid FM index partition reserved field.");

        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        BlockInfo directory = readBlockInfo(data);
        long firstRowId = data.readLong();
        int rowCount = data.readInt();
        int textLength = data.readInt();
        int sampleRate = data.readInt();
        Preconditions.checkState(firstRowId >= 0 && rowCount > 0, "Invalid FM index row range.");
        Preconditions.checkState(
                firstRowId <= Long.MAX_VALUE - rowCount,
                "FM index row range overflows the supported row ID space.");
        Preconditions.checkState(
                textLength >= rowCount + 1L, "Invalid FM index encoded text length.");
        validateSampleRate(sampleRate);
        validateBlock(directory, footerOffset, MAX_DIRECTORY_UNCOMPRESSED_LENGTH, false);
        Preconditions.checkState(
                directory.offset >= partition.startOffset
                        && directory.offset + directory.storedLength == footerOffset,
                "FM index partition directory is not immediately before its footer.");
        Preconditions.checkState(
                firstRowId == partition.firstRowId && rowCount == partition.rowCount,
                "FM index partition footer and container directory metadata do not match.");
        return new Footer(
                directory,
                firstRowId,
                rowCount,
                textLength,
                sampleRate,
                partition.startOffset,
                partition.endOffset);
    }

    static Footer readFooter(SeekableInputStream input, long fileSize) throws IOException {
        ContainerFooter containerFooter = readContainerFooter(input, fileSize);
        IndexMeta metadata = readContainerDirectory(input, containerFooter, fileSize);
        return readFooter(input, metadata.partitions.get(0), fileSize);
    }

    static Directory readDirectory(SeekableInputStream input, Footer footer, long fileSize)
            throws IOException {
        Preconditions.checkState(
                footer.partitionStartOffset >= 0
                        && footer.partitionEndOffset <= fileSize
                        && footer.partitionStartOffset < footer.partitionEndOffset,
                "Invalid FM index partition range.");
        byte[] bytes = readBlock(input, footer.directory, footer.partitionEndOffset);
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        int rowCount = data.readInt();
        int textLength = data.readInt();
        int sampleRate = data.readInt();
        Preconditions.checkState(
                rowCount == footer.rowCount
                        && textLength == footer.textLength
                        && sampleRate == footer.sampleRate,
                "FM index footer and directory metadata do not match.");
        int levelCount = data.readInt();
        int alphabetSize = data.readInt();
        Preconditions.checkState(
                alphabetSize >= FIRST_BYTE_SYMBOL
                        && alphabetSize <= MAX_ALPHABET_SIZE
                        && levelCount == levelsForAlphabet(alphabetSize)
                        && data.readInt() == BLOCK_WORDS,
                "Unsupported FM index physical layout.");

        int[] byteToSymbol = new int[256];
        int nextDenseSymbol = FIRST_BYTE_SYMBOL;
        for (int i = 0; i < byteToSymbol.length; i++) {
            int symbol = data.readInt();
            Preconditions.checkState(
                    symbol == -1 || symbol == nextDenseSymbol,
                    "Invalid FM index dense byte alphabet.");
            byteToSymbol[i] = symbol;
            if (symbol >= 0) {
                nextDenseSymbol++;
            }
        }
        Preconditions.checkState(
                nextDenseSymbol == alphabetSize,
                "FM index dense byte alphabet does not match its alphabet size.");

        int[] cumulative = new int[alphabetSize + 1];
        for (int i = 0; i < cumulative.length; i++) {
            cumulative[i] = data.readInt();
        }
        Preconditions.checkState(
                cumulative[0] == 0 && cumulative[alphabetSize] == textLength,
                "Invalid FM index cumulative counts.");
        for (int i = 1; i < cumulative.length; i++) {
            Preconditions.checkState(
                    cumulative[i] >= cumulative[i - 1],
                    "FM index cumulative counts are not ordered.");
        }
        Preconditions.checkState(
                cumulative[1] - cumulative[0] == 1,
                "FM index must contain exactly one terminator.");
        Preconditions.checkState(
                cumulative[SEPARATOR + 1] - cumulative[SEPARATOR] == rowCount,
                "FM index separator count does not match its row count.");
        for (int symbol : byteToSymbol) {
            if (symbol >= 0) {
                Preconditions.checkState(
                        cumulative[symbol + 1] > cumulative[symbol],
                        "FM index alphabet contains an unused byte symbol.");
            }
        }

        long[] expectedOffset = {footer.partitionStartOffset};
        int[][] digitStarts = new int[levelCount][4];
        QuadVectorMeta[] wavelets = new QuadVectorMeta[levelCount];
        for (int i = 0; i < wavelets.length; i++) {
            int[] expectedDigitCounts = expectedDigitCounts(cumulative, (levelCount - i - 1) * 2);
            int start = 0;
            for (int digit = 0; digit < 4; digit++) {
                digitStarts[i][digit] = data.readInt();
                Preconditions.checkState(
                        digitStarts[i][digit] == start,
                        "Invalid FM index quaternary wavelet digit start.");
                start += expectedDigitCounts[digit];
            }
            Preconditions.checkState(
                    start == textLength, "FM index quaternary digits do not cover the text.");
            wavelets[i] =
                    readQuadVectorMeta(
                            data,
                            textLength,
                            expectedDigitCounts,
                            expectedOffset,
                            footer.directory.offset);
        }
        BitVectorMeta sampledRows =
                readBitVectorMeta(data, textLength, expectedOffset, footer.directory.offset);
        int expectedSamples = ((textLength - 1) / sampleRate) + 1;
        Preconditions.checkState(
                sampledRows.totalOnes == expectedSamples,
                "FM index sampled-row cardinality does not match its sampling rate.");
        IntVectorMeta sampleValues =
                readIntVectorMeta(data, expectedSamples, expectedOffset, footer.directory.offset);
        BitVectorMeta nullRows =
                readBitVectorMeta(data, rowCount, expectedOffset, footer.directory.offset);
        BitVectorMeta rowBoundaries =
                readBitVectorMeta(data, textLength, expectedOffset, footer.directory.offset);
        Preconditions.checkState(
                rowBoundaries.totalOnes == rowCount,
                "FM index row-boundary cardinality does not match its row count.");
        int verificationPageCount = data.readInt();
        Preconditions.checkState(
                verificationPageCount > 0 && verificationPageCount <= rowCount,
                "Invalid FM index verification page count.");
        List<VerificationPageMeta> verificationPages = new ArrayList<>(verificationPageCount);
        int nextRow = 0;
        for (int i = 0; i < verificationPageCount; i++) {
            int firstRow = data.readInt();
            int pageRowCount = data.readInt();
            BlockInfo block = readBlockInfo(data);
            Preconditions.checkState(
                    firstRow == nextRow
                            && pageRowCount > 0
                            && pageRowCount <= rowCount - firstRow
                            && block.uncompressedLength >= pageRowCount * Integer.BYTES
                            && block.uncompressedLength
                                    <= MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH,
                    "Invalid FM index verification page metadata.");
            validateCanonicalBlock(
                    block,
                    expectedOffset,
                    footer.directory.offset,
                    MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH);
            verificationPages.add(new VerificationPageMeta(firstRow, pageRowCount, block));
            nextRow += pageRowCount;
        }
        Preconditions.checkState(
                nextRow == rowCount, "FM index verification pages do not cover all rows.");
        Preconditions.checkState(
                expectedOffset[0] == footer.directory.offset,
                "FM index payload blocks are not canonical and contiguous.");
        Preconditions.checkState(
                data.available() == 0, "FM index directory contains trailing bytes.");
        return new Directory(
                rowCount,
                textLength,
                sampleRate,
                levelCount,
                alphabetSize,
                byteToSymbol,
                cumulative,
                digitStarts,
                wavelets,
                sampledRows,
                sampleValues,
                nullRows,
                rowBoundaries,
                verificationPages);
    }

    static byte[] readBlock(SeekableInputStream input, BlockInfo block, long fileSize)
            throws IOException {
        validateBlock(block, fileSize, MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH, true);
        byte[] stored = readAt(input, block.offset, block.storedLength);
        return decodeStoredBlock(stored, 0, block);
    }

    static List<byte[]> readBlocks(SeekableInputStream input, List<BlockInfo> blocks, long fileSize)
            throws IOException {
        Preconditions.checkArgument(!blocks.isEmpty(), "FM demand page must contain blocks.");
        long firstOffset = blocks.get(0).offset;
        long nextOffset = firstOffset;
        long totalStoredLength = 0;
        for (BlockInfo block : blocks) {
            validateBlock(block, fileSize, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH, false);
            Preconditions.checkState(
                    block.offset == nextOffset,
                    "FM demand-page blocks are not canonical and contiguous.");
            totalStoredLength += block.storedLength;
            Preconditions.checkState(
                    totalStoredLength <= Integer.MAX_VALUE,
                    "FM demand page exceeds the supported read size.");
            nextOffset += block.storedLength;
        }
        byte[] stored = readAt(input, firstOffset, (int) totalStoredLength);
        List<byte[]> result = new ArrayList<>(blocks.size());
        int offset = 0;
        for (BlockInfo block : blocks) {
            result.add(decodeStoredBlock(stored, offset, block));
            offset += block.storedLength;
        }
        return result;
    }

    static byte[] readVerificationBlockRange(
            SeekableInputStream input, List<BlockInfo> blocks, long fileSize) throws IOException {
        Preconditions.checkArgument(
                !blocks.isEmpty(), "FM verification range must contain blocks.");
        long firstOffset = blocks.get(0).offset;
        long nextOffset = firstOffset;
        long totalStoredLength = 0;
        for (BlockInfo block : blocks) {
            validateVerificationBlock(block, fileSize);
            Preconditions.checkState(
                    block.offset == nextOffset,
                    "FM verification blocks are not canonical and contiguous.");
            totalStoredLength += block.storedLength;
            Preconditions.checkState(
                    totalStoredLength <= Integer.MAX_VALUE,
                    "FM verification range exceeds the supported read size.");
            nextOffset += block.storedLength;
        }
        return readAt(input, firstOffset, (int) totalStoredLength);
    }

    static List<byte[]> decodeVerificationBlockRange(byte[] stored, List<BlockInfo> blocks) {
        List<byte[]> result = new ArrayList<>(blocks.size());
        int offset = 0;
        for (BlockInfo block : blocks) {
            result.add(decodeStoredBlock(stored, offset, block));
            offset += block.storedLength;
        }
        Preconditions.checkState(
                offset == stored.length, "FM verification range contains trailing bytes.");
        return result;
    }

    static void validateVerificationBlock(BlockInfo block, long fileSize) {
        validateBlock(block, fileSize, MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH, false);
    }

    private static byte[] decodeStoredBlock(byte[] stored, int offset, BlockInfo block) {
        Preconditions.checkState(
                offset >= 0 && block.storedLength <= stored.length - offset,
                "FM demand-page block exceeds its stored bytes.");
        BlockCompressionType compression = compression(block.compressionId);
        int checksum =
                crc32c(
                        new MemorySlice(MemorySegment.wrap(stored), offset, block.storedLength),
                        compression);
        Preconditions.checkState(
                checksum == block.checksum,
                "FM index block checksum mismatch: expected=%s, actual=%s.",
                block.checksum,
                checksum);
        if (compression == BlockCompressionType.NONE) {
            Preconditions.checkState(
                    block.storedLength == block.uncompressedLength,
                    "Invalid uncompressed FM index block length.");
            byte[] result = new byte[block.storedLength];
            System.arraycopy(stored, offset, result, 0, result.length);
            return result;
        }
        BlockCompressionFactory factory = BlockCompressionFactory.create(compression);
        Preconditions.checkState(factory != null, "Missing FM index decompressor.");
        byte[] result = new byte[block.uncompressedLength];
        BlockDecompressor decompressor = factory.getDecompressor();
        int length = decompressor.decompress(stored, offset, block.storedLength, result, 0);
        Preconditions.checkState(
                length == result.length, "FM index block decompressed length mismatch.");
        return result;
    }

    static QuadBlock decodeQuadBlock(byte[] bytes, QuadBlockMeta meta) throws IOException {
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        int wordCount = data.readInt();
        int prefixCount = data.readInt();
        int expectedWords = wordsForQuads(meta.valueCount);
        int expectedPrefixes = ((wordCount + 63) / 64) + 1;
        Preconditions.checkState(
                wordCount == expectedWords && prefixCount == expectedPrefixes,
                "Invalid FM index quaternary rank block header.");
        int[] prefixes = new int[prefixCount * 4];
        for (int i = 0; i < prefixes.length; i++) {
            prefixes[i] = data.readInt();
        }
        long[] words = new long[wordCount];
        int[] counts = new int[4];
        for (int i = 0; i < wordCount; i++) {
            if ((i & 63) == 0) {
                int prefix = (i / 64) * 4;
                for (int digit = 0; digit < 4; digit++) {
                    Preconditions.checkState(
                            prefixes[prefix + digit] == counts[digit],
                            "Invalid FM index quaternary rank prefixes.");
                }
            }
            words[i] = data.readLong();
            int validValues =
                    Math.min(QUAD_VALUES_PER_WORD, meta.valueCount - i * QUAD_VALUES_PER_WORD);
            for (int digit = 0; digit < 4; digit++) {
                counts[digit] += countDigit(words[i], digit, validValues);
            }
        }
        int lastPrefix = (prefixCount - 1) * 4;
        for (int digit = 0; digit < 4; digit++) {
            Preconditions.checkState(
                    prefixes[lastPrefix + digit] == counts[digit]
                            && counts[digit] == meta.counts[digit],
                    "FM index quaternary rank cardinality mismatch.");
        }
        Preconditions.checkState(
                data.available() == 0, "FM index quaternary rank block has trailing bytes.");
        int remaining = meta.valueCount & (QUAD_VALUES_PER_WORD - 1);
        if (remaining != 0) {
            long paddingMask = ~((1L << (remaining * 2)) - 1L);
            Preconditions.checkState(
                    (words[words.length - 1] & paddingMask) == 0,
                    "FM index quaternary block has non-zero padding digits.");
        }
        return new QuadBlock(words, prefixes, meta.valueCount);
    }

    static BitBlock decodeBitBlock(byte[] bytes, BitBlockMeta meta) throws IOException {
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        int wordCount = data.readInt();
        int prefixCount = data.readInt();
        int expectedWords = wordsForBits(meta.bitCount);
        int expectedPrefixes = ((wordCount + 63) / 64) + 1;
        Preconditions.checkState(
                wordCount == expectedWords && prefixCount == expectedPrefixes,
                "Invalid FM index rank block header.");
        int[] prefixes = new int[prefixCount];
        for (int i = 0; i < prefixCount; i++) {
            prefixes[i] = data.readInt();
        }
        long[] words = new long[wordCount];
        int ones = 0;
        for (int i = 0; i < wordCount; i++) {
            if ((i & 63) == 0) {
                Preconditions.checkState(
                        prefixes[i / 64] == ones, "Invalid FM index rank block prefix counts.");
            }
            words[i] = data.readLong();
            ones += Long.bitCount(words[i]);
        }
        Preconditions.checkState(
                prefixes[prefixCount - 1] == ones && ones == meta.onesCount,
                "FM index rank block cardinality mismatch.");
        Preconditions.checkState(data.available() == 0, "FM index rank block has trailing bytes.");
        int remaining = meta.bitCount & 63;
        if (remaining != 0) {
            long paddingMask = ~((1L << remaining) - 1L);
            Preconditions.checkState(
                    (words[words.length - 1] & paddingMask) == 0,
                    "FM index rank block has non-zero padding bits.");
        }
        return new BitBlock(words, prefixes, meta.bitCount);
    }

    static int[] decodeIntBlock(byte[] bytes, IntBlockMeta meta) throws IOException {
        Preconditions.checkState(
                bytes.length == meta.valueCount * Integer.BYTES,
                "Invalid FM index sample block length.");
        DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes));
        int[] values = new int[meta.valueCount];
        for (int i = 0; i < values.length; i++) {
            values[i] = data.readInt();
        }
        return values;
    }

    private static byte[] encodeBitBlock(long[] words, int start, int count) throws IOException {
        int prefixCount = ((count + 63) / 64) + 1;
        ByteArrayOutputStream bytes =
                new ByteArrayOutputStream((2 + prefixCount) * Integer.BYTES + count * Long.BYTES);
        DataOutputStream data = new DataOutputStream(bytes);
        data.writeInt(count);
        data.writeInt(prefixCount);
        int ones = 0;
        for (int group = 0; group < prefixCount; group++) {
            data.writeInt(ones);
            int end = Math.min(count, (group + 1) * 64);
            for (int i = group * 64; i < end; i++) {
                ones += Long.bitCount(words[start + i]);
            }
        }
        for (int i = 0; i < count; i++) {
            data.writeLong(words[start + i]);
        }
        data.flush();
        return bytes.toByteArray();
    }

    private static byte[] encodeQuadBlock(long[] words, int start, int count, int valueCount)
            throws IOException {
        int prefixCount = ((count + 63) / 64) + 1;
        ByteArrayOutputStream bytes =
                new ByteArrayOutputStream(
                        (2 + prefixCount * 4) * Integer.BYTES + count * Long.BYTES);
        DataOutputStream data = new DataOutputStream(bytes);
        data.writeInt(count);
        data.writeInt(prefixCount);
        int[] counts = new int[4];
        for (int group = 0; group < prefixCount; group++) {
            for (int digit = 0; digit < 4; digit++) {
                data.writeInt(counts[digit]);
            }
            int end = Math.min(count, (group + 1) * 64);
            for (int i = group * 64; i < end; i++) {
                int validValues =
                        Math.min(QUAD_VALUES_PER_WORD, valueCount - i * QUAD_VALUES_PER_WORD);
                for (int digit = 0; digit < 4; digit++) {
                    counts[digit] += countDigit(words[start + i], digit, validValues);
                }
            }
        }
        for (int i = 0; i < count; i++) {
            data.writeLong(words[start + i]);
        }
        data.flush();
        return bytes.toByteArray();
    }

    private static void writeQuadVectorMeta(DataOutputStream out, QuadVectorMeta vector)
            throws IOException {
        out.writeInt(vector.valueLength);
        for (int count : vector.totalCounts) {
            out.writeInt(count);
        }
        out.writeInt(vector.blocks.size());
        for (QuadBlockMeta block : vector.blocks) {
            out.writeInt(block.firstValue);
            out.writeInt(block.valueCount);
            for (int count : block.prefixCounts) {
                out.writeInt(count);
            }
            for (int count : block.counts) {
                out.writeInt(count);
            }
            writeBlockInfo(out, block.block);
        }
    }

    private static QuadVectorMeta readQuadVectorMeta(
            DataInputStream data,
            int expectedValueLength,
            int[] expectedTotalCounts,
            long[] expectedOffset,
            long payloadEnd)
            throws IOException {
        int valueLength = data.readInt();
        int[] totalCounts = new int[4];
        int total = 0;
        for (int digit = 0; digit < 4; digit++) {
            totalCounts[digit] = data.readInt();
            Preconditions.checkState(
                    totalCounts[digit] == expectedTotalCounts[digit],
                    "Invalid FM index quaternary digit cardinality.");
            total += totalCounts[digit];
        }
        int blockCount = data.readInt();
        Preconditions.checkState(
                valueLength == expectedValueLength
                        && total == valueLength
                        && blockCount == blocksForQuads(valueLength),
                "Invalid FM index quaternary-vector metadata.");
        List<QuadBlockMeta> blocks = new ArrayList<>(blockCount);
        int firstValue = 0;
        int[] prefixCounts = new int[4];
        for (int i = 0; i < blockCount; i++) {
            int storedFirstValue = data.readInt();
            int valueCount = data.readInt();
            int expectedValues = Math.min(QUAD_BLOCK_VALUES, valueLength - firstValue);
            int[] storedPrefixes = new int[4];
            int[] counts = new int[4];
            int blockTotal = 0;
            for (int digit = 0; digit < 4; digit++) {
                storedPrefixes[digit] = data.readInt();
                Preconditions.checkState(
                        storedPrefixes[digit] == prefixCounts[digit],
                        "Invalid FM index quaternary block prefix.");
            }
            for (int digit = 0; digit < 4; digit++) {
                counts[digit] = data.readInt();
                Preconditions.checkState(
                        counts[digit] >= 0, "Invalid FM index quaternary block cardinality.");
                blockTotal += counts[digit];
            }
            BlockInfo block = readBlockInfo(data);
            Preconditions.checkState(
                    storedFirstValue == firstValue
                            && valueCount == expectedValues
                            && blockTotal == valueCount
                            && block.uncompressedLength == encodedQuadBlockLength(valueCount),
                    "Invalid FM index quaternary block metadata.");
            validateCanonicalBlock(
                    block, expectedOffset, payloadEnd, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH);
            blocks.add(new QuadBlockMeta(firstValue, valueCount, storedPrefixes, counts, block));
            for (int digit = 0; digit < 4; digit++) {
                prefixCounts[digit] += counts[digit];
            }
            firstValue += valueCount;
        }
        Preconditions.checkState(
                firstValue == valueLength && java.util.Arrays.equals(prefixCounts, totalCounts),
                "FM index quaternary blocks do not cover the vector.");
        return new QuadVectorMeta(valueLength, totalCounts, blocks);
    }

    private static void writeBitVectorMeta(DataOutputStream out, BitVectorMeta vector)
            throws IOException {
        out.writeInt(vector.bitLength);
        out.writeInt(vector.totalOnes);
        out.writeInt(vector.blocks.size());
        for (BitBlockMeta block : vector.blocks) {
            out.writeInt(block.firstBit);
            out.writeInt(block.bitCount);
            out.writeInt(block.prefixOnes);
            out.writeInt(block.onesCount);
            writeBlockInfo(out, block.block);
        }
    }

    private static BitVectorMeta readBitVectorMeta(
            DataInputStream data, int expectedBitLength, long[] expectedOffset, long payloadEnd)
            throws IOException {
        int bitLength = data.readInt();
        int totalOnes = data.readInt();
        int blockCount = data.readInt();
        Preconditions.checkState(
                bitLength == expectedBitLength
                        && totalOnes >= 0
                        && totalOnes <= bitLength
                        && blockCount == blocksForBits(bitLength),
                "Invalid FM index bit-vector metadata.");
        List<BitBlockMeta> blocks = new ArrayList<>(blockCount);
        int firstBit = 0;
        int prefixOnes = 0;
        for (int i = 0; i < blockCount; i++) {
            int storedFirstBit = data.readInt();
            int bitCount = data.readInt();
            int storedPrefixOnes = data.readInt();
            int onesCount = data.readInt();
            BlockInfo block = readBlockInfo(data);
            int expectedBits = Math.min(BLOCK_BITS, bitLength - firstBit);
            Preconditions.checkState(
                    storedFirstBit == firstBit
                            && bitCount == expectedBits
                            && storedPrefixOnes == prefixOnes
                            && onesCount >= 0
                            && onesCount <= bitCount,
                    "Invalid FM index bit block metadata.");
            int expectedLength = encodedBitBlockLength(bitCount);
            Preconditions.checkState(
                    block.uncompressedLength == expectedLength,
                    "Invalid FM index rank block length.");
            validateCanonicalBlock(
                    block, expectedOffset, payloadEnd, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH);
            blocks.add(new BitBlockMeta(firstBit, bitCount, prefixOnes, onesCount, block));
            firstBit += bitCount;
            prefixOnes += onesCount;
        }
        Preconditions.checkState(
                firstBit == bitLength && prefixOnes == totalOnes,
                "FM index bit-vector blocks do not match their summary.");
        return new BitVectorMeta(bitLength, totalOnes, blocks);
    }

    private static void writeIntVectorMeta(DataOutputStream out, IntVectorMeta vector)
            throws IOException {
        out.writeInt(vector.valueCount);
        out.writeInt(vector.blocks.size());
        for (IntBlockMeta block : vector.blocks) {
            out.writeInt(block.firstValue);
            out.writeInt(block.valueCount);
            writeBlockInfo(out, block.block);
        }
    }

    private static IntVectorMeta readIntVectorMeta(
            DataInputStream data, int expectedValueCount, long[] expectedOffset, long payloadEnd)
            throws IOException {
        int valueCount = data.readInt();
        int blockCount = data.readInt();
        Preconditions.checkState(
                valueCount == expectedValueCount && blockCount == blocksForValues(valueCount),
                "Invalid FM index sample vector metadata.");
        List<IntBlockMeta> blocks = new ArrayList<>(blockCount);
        int firstValue = 0;
        for (int i = 0; i < blockCount; i++) {
            int storedFirstValue = data.readInt();
            int count = data.readInt();
            int expectedCount = Math.min(VALUE_BLOCK_INTS, valueCount - firstValue);
            BlockInfo block = readBlockInfo(data);
            Preconditions.checkState(
                    storedFirstValue == firstValue
                            && count == expectedCount
                            && block.uncompressedLength == count * Integer.BYTES,
                    "Invalid FM index sample block metadata.");
            validateCanonicalBlock(
                    block, expectedOffset, payloadEnd, MAX_DATA_BLOCK_UNCOMPRESSED_LENGTH);
            blocks.add(new IntBlockMeta(firstValue, count, block));
            firstValue += count;
        }
        Preconditions.checkState(
                firstValue == valueCount, "FM index sample blocks do not cover all samples.");
        return new IntVectorMeta(valueCount, blocks);
    }

    private static void validateCanonicalBlock(
            BlockInfo block, long[] expectedOffset, long payloadEnd, int maxUncompressedLength) {
        validateBlock(block, payloadEnd, maxUncompressedLength, false);
        Preconditions.checkState(
                block.offset == expectedOffset[0],
                "FM index payload blocks are aliased, reordered or contain gaps.");
        expectedOffset[0] += block.storedLength;
    }

    private static void validateBlock(
            BlockInfo block, long payloadEnd, int maxUncompressedLength, boolean allowDirectory) {
        Preconditions.checkState(
                block.offset >= 0
                        && block.storedLength > 0
                        && block.uncompressedLength > 0
                        && block.storedLength <= block.uncompressedLength
                        && block.uncompressedLength <= maxUncompressedLength
                        && block.offset <= payloadEnd
                        && block.storedLength <= payloadEnd - block.offset,
                allowDirectory
                        ? "Invalid FM index block metadata."
                        : "Invalid FM index data block metadata.");
        compression(block.compressionId);
    }

    private static BlockCompressionType compression(int persistentId) {
        try {
            return BlockCompressionType.getCompressionTypeByPersistentId(persistentId);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unknown FM index compression ID: " + persistentId, e);
        }
    }

    static int levelsForAlphabet(int alphabetSize) {
        Preconditions.checkArgument(
                alphabetSize >= FIRST_BYTE_SYMBOL && alphabetSize <= MAX_ALPHABET_SIZE,
                "Invalid FM index alphabet size.");
        int highestSymbol = alphabetSize - 1;
        int bits = Integer.SIZE - Integer.numberOfLeadingZeros(highestSymbol);
        return Math.max(1, (bits + 1) / 2);
    }

    private static int[] expectedDigitCounts(int[] cumulative, int shift) {
        int[] counts = new int[4];
        for (int symbol = 0; symbol < cumulative.length - 1; symbol++) {
            counts[(symbol >>> shift) & 3] += cumulative[symbol + 1] - cumulative[symbol];
        }
        return counts;
    }

    private static int[] waveletStarts(int[] cumulative, int levelCount) {
        int alphabetSize = cumulative.length - 1;
        int[] starts = new int[alphabetSize];
        int offset = 0;
        int physicalAlphabetSize = 1 << (levelCount * 2);
        for (int order = 0; order < physicalAlphabetSize; order++) {
            int value = order;
            int symbol = 0;
            for (int level = 0; level < levelCount; level++) {
                symbol = (symbol << 2) | (value & 3);
                value >>>= 2;
            }
            if (symbol < alphabetSize) {
                starts[symbol] = offset;
                offset += cumulative[symbol + 1] - cumulative[symbol];
            }
        }
        Preconditions.checkState(offset == cumulative[alphabetSize], "Invalid FM wavelet starts.");
        return starts;
    }

    private static int footerChecksum(byte[] footer) {
        return crc32c(
                new MemorySlice(MemorySegment.wrap(footer), 0, FOOTER_CHECKSUM_OFFSET),
                BlockCompressionType.NONE);
    }

    private static int indexMetaChecksum(byte[] metadata) {
        return crc32c(
                new MemorySlice(
                        MemorySegment.wrap(metadata),
                        0,
                        metadata.length - INDEX_META_CHECKSUM_LENGTH),
                BlockCompressionType.NONE);
    }

    private static byte[] readAt(SeekableInputStream input, long offset, int length)
            throws IOException {
        byte[] result = new byte[length];
        if (input instanceof VectoredReadable) {
            ((VectoredReadable) input).preadFully(offset, result, 0, length);
            return result;
        }
        input.seek(offset);
        int read = 0;
        while (read < length) {
            int count = input.read(result, read, length - read);
            if (count < 0) {
                throw new EOFException("FM index file ended before the requested block was read.");
            }
            read += count;
        }
        return result;
    }

    private static int blocksForBits(int bitLength) {
        return (int) (((long) bitLength + BLOCK_BITS - 1) / BLOCK_BITS);
    }

    private static int blocksForQuads(int valueLength) {
        return (int) (((long) valueLength + QUAD_BLOCK_VALUES - 1) / QUAD_BLOCK_VALUES);
    }

    private static int blocksForValues(int valueCount) {
        return (int) (((long) valueCount + VALUE_BLOCK_INTS - 1) / VALUE_BLOCK_INTS);
    }

    private static int wordsForBits(int bitLength) {
        return (int) (((long) bitLength + Long.SIZE - 1) / Long.SIZE);
    }

    static int wordsForQuads(int valueLength) {
        return (int) (((long) valueLength + QUAD_VALUES_PER_WORD - 1) / QUAD_VALUES_PER_WORD);
    }

    private static int encodedBitBlockLength(int bitCount) {
        int words = wordsForBits(bitCount);
        int prefixes = ((words + 63) / 64) + 1;
        return (2 + prefixes) * Integer.BYTES + words * Long.BYTES;
    }

    private static int encodedQuadBlockLength(int valueCount) {
        int words = wordsForQuads(valueCount);
        int prefixes = ((words + 63) / 64) + 1;
        return (2 + prefixes * 4) * Integer.BYTES + words * Long.BYTES;
    }

    private static int countDigit(long word, int digit, int validValues) {
        if (validValues <= 0) {
            return 0;
        }
        long repeated = digit * 0x5555555555555555L;
        long different = word ^ repeated;
        long matches = ~(different | (different >>> 1)) & 0x5555555555555555L;
        return Long.bitCount(matches & lowQuadMask(validValues));
    }

    private static long lowQuadMask(int values) {
        if (values >= QUAD_VALUES_PER_WORD) {
            return 0x5555555555555555L;
        }
        return 0x5555555555555555L & ((1L << (values * 2)) - 1L);
    }

    private static void validateSampleRate(int sampleRate) {
        Preconditions.checkState(
                sampleRate > 0 && sampleRate <= 1024 && (sampleRate & (sampleRate - 1)) == 0,
                "FM index SA sample rate must be a power of two in [1, 1024].");
    }

    private static void writeBlockInfo(DataOutputStream out, BlockInfo block) throws IOException {
        out.writeLong(block.offset);
        out.writeInt(block.storedLength);
        out.writeInt(block.uncompressedLength);
        out.writeInt(block.compressionId);
        out.writeInt(block.checksum);
    }

    private static BlockInfo readBlockInfo(DataInputStream in) throws IOException {
        return new BlockInfo(in.readLong(), in.readInt(), in.readInt(), in.readInt(), in.readInt());
    }

    private static void writeBlockInfo(byte[] bytes, int offset, BlockInfo block) {
        writeLong(bytes, offset, block.offset);
        writeInt(bytes, offset + 8, block.storedLength);
        writeInt(bytes, offset + 12, block.uncompressedLength);
        writeInt(bytes, offset + 16, block.compressionId);
        writeInt(bytes, offset + 20, block.checksum);
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private static long readLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (bytes[offset + i] & 0xFFL);
        }
        return value;
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static void writeLong(byte[] bytes, int offset, long value) {
        for (int i = 7; i >= 0; i--) {
            bytes[offset + i] = (byte) value;
            value >>>= 8;
        }
    }

    static final class BlockInfo {
        final long offset;
        final int storedLength;
        final int uncompressedLength;
        final int compressionId;
        final int checksum;

        BlockInfo(
                long offset,
                int storedLength,
                int uncompressedLength,
                int compressionId,
                int checksum) {
            this.offset = offset;
            this.storedLength = storedLength;
            this.uncompressedLength = uncompressedLength;
            this.compressionId = compressionId;
            this.checksum = checksum;
        }
    }

    static final class QuadBlockMeta {
        final int firstValue;
        final int valueCount;
        final int[] prefixCounts;
        final int[] counts;
        final BlockInfo block;

        QuadBlockMeta(
                int firstValue, int valueCount, int[] prefixCounts, int[] counts, BlockInfo block) {
            this.firstValue = firstValue;
            this.valueCount = valueCount;
            this.prefixCounts = prefixCounts;
            this.counts = counts;
            this.block = block;
        }
    }

    static final class QuadVectorMeta {
        final int valueLength;
        final int[] totalCounts;
        final List<QuadBlockMeta> blocks;

        QuadVectorMeta(int valueLength, int[] totalCounts, List<QuadBlockMeta> blocks) {
            this.valueLength = valueLength;
            this.totalCounts = totalCounts;
            this.blocks = blocks;
        }

        QuadBlockMeta block(int valuePosition) {
            Preconditions.checkArgument(
                    valuePosition >= 0 && valuePosition < valueLength,
                    "FM index quaternary position is outside the vector.");
            return blocks.get(valuePosition / QUAD_BLOCK_VALUES);
        }
    }

    static final class BitBlockMeta {
        final int firstBit;
        final int bitCount;
        final int prefixOnes;
        final int onesCount;
        final BlockInfo block;

        BitBlockMeta(int firstBit, int bitCount, int prefixOnes, int onesCount, BlockInfo block) {
            this.firstBit = firstBit;
            this.bitCount = bitCount;
            this.prefixOnes = prefixOnes;
            this.onesCount = onesCount;
            this.block = block;
        }
    }

    static final class BitVectorMeta {
        final int bitLength;
        final int totalOnes;
        final List<BitBlockMeta> blocks;

        BitVectorMeta(int bitLength, int totalOnes, List<BitBlockMeta> blocks) {
            this.bitLength = bitLength;
            this.totalOnes = totalOnes;
            this.blocks = blocks;
        }

        BitBlockMeta block(int bitPosition) {
            Preconditions.checkArgument(
                    bitPosition >= 0 && bitPosition < bitLength,
                    "FM index bit position is outside the vector.");
            return blocks.get(bitPosition / BLOCK_BITS);
        }
    }

    static final class IntBlockMeta {
        final int firstValue;
        final int valueCount;
        final BlockInfo block;

        IntBlockMeta(int firstValue, int valueCount, BlockInfo block) {
            this.firstValue = firstValue;
            this.valueCount = valueCount;
            this.block = block;
        }
    }

    static final class IntVectorMeta {
        final int valueCount;
        final List<IntBlockMeta> blocks;

        IntVectorMeta(int valueCount, List<IntBlockMeta> blocks) {
            this.valueCount = valueCount;
            this.blocks = blocks;
        }

        IntBlockMeta block(int valuePosition) {
            Preconditions.checkArgument(
                    valuePosition >= 0 && valuePosition < valueCount,
                    "FM index sample position is outside the vector.");
            return blocks.get(valuePosition / VALUE_BLOCK_INTS);
        }
    }

    static final class Directory {
        final int rowCount;
        final int textLength;
        final int sampleRate;
        final int levelCount;
        final int alphabetSize;
        final int[] byteToSymbol;
        final int[] cumulativeCounts;
        final int[] waveletStarts;
        final int[][] digitStarts;
        final QuadVectorMeta[] wavelets;
        final BitVectorMeta sampledRows;
        final IntVectorMeta sampleValues;
        final BitVectorMeta nullRows;
        final BitVectorMeta rowBoundaries;
        final List<VerificationPageMeta> verificationPages;

        Directory(
                int rowCount,
                int textLength,
                int sampleRate,
                int levelCount,
                int alphabetSize,
                int[] byteToSymbol,
                int[] cumulativeCounts,
                int[][] digitStarts,
                QuadVectorMeta[] wavelets,
                BitVectorMeta sampledRows,
                IntVectorMeta sampleValues,
                BitVectorMeta nullRows,
                BitVectorMeta rowBoundaries,
                List<VerificationPageMeta> verificationPages) {
            this.rowCount = rowCount;
            this.textLength = textLength;
            this.sampleRate = sampleRate;
            this.levelCount = levelCount;
            this.alphabetSize = alphabetSize;
            this.byteToSymbol = byteToSymbol;
            this.cumulativeCounts = cumulativeCounts;
            this.waveletStarts = waveletStarts(cumulativeCounts, levelCount);
            this.digitStarts = digitStarts;
            this.wavelets = wavelets;
            this.sampledRows = sampledRows;
            this.sampleValues = sampleValues;
            this.nullRows = nullRows;
            this.rowBoundaries = rowBoundaries;
            this.verificationPages = verificationPages;
        }
    }

    static final class VerificationPageMeta {
        final int firstRow;
        final int rowCount;
        final BlockInfo block;

        VerificationPageMeta(int firstRow, int rowCount, BlockInfo block) {
            this.firstRow = firstRow;
            this.rowCount = rowCount;
            this.block = block;
        }
    }

    static final class Footer {
        final BlockInfo directory;
        final long firstRowId;
        final int rowCount;
        final int textLength;
        final int sampleRate;
        final long partitionStartOffset;
        final long partitionEndOffset;

        Footer(
                BlockInfo directory,
                long firstRowId,
                int rowCount,
                int textLength,
                int sampleRate,
                long partitionStartOffset,
                long partitionEndOffset) {
            this.directory = directory;
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.textLength = textLength;
            this.sampleRate = sampleRate;
            this.partitionStartOffset = partitionStartOffset;
            this.partitionEndOffset = partitionEndOffset;
        }
    }

    static final class ContainerFooter {
        final BlockInfo directory;
        final long firstRowId;
        final long rowCount;
        final int partitionCount;

        private ContainerFooter(
                BlockInfo directory, long firstRowId, long rowCount, int partitionCount) {
            this.directory = directory;
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.partitionCount = partitionCount;
        }
    }

    static final class PartitionMeta {
        final long startOffset;
        final long endOffset;
        final long firstRowId;
        final int rowCount;

        PartitionMeta(long startOffset, long endOffset, long firstRowId, int rowCount) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
        }

        long lastRowId() {
            return firstRowId + rowCount - 1L;
        }
    }

    static final class IndexMeta {
        final long firstRowId;
        final long rowCount;
        final List<PartitionMeta> partitions;

        private IndexMeta(long firstRowId, long rowCount, List<PartitionMeta> partitions) {
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        }

        long lastRowId() {
            return firstRowId + rowCount - 1L;
        }

        boolean sameLayout(IndexMeta that) {
            if (firstRowId != that.firstRowId
                    || rowCount != that.rowCount
                    || partitions.size() != that.partitions.size()) {
                return false;
            }
            for (int i = 0; i < partitions.size(); i++) {
                PartitionMeta left = partitions.get(i);
                PartitionMeta right = that.partitions.get(i);
                if (left.startOffset != right.startOffset
                        || left.endOffset != right.endOffset
                        || left.firstRowId != right.firstRowId
                        || left.rowCount != right.rowCount) {
                    return false;
                }
            }
            return true;
        }
    }

    static final class QuadBlock {
        private final long[] words;
        private final int[] prefixes;
        private final int valueCount;

        QuadBlock(long[] words, int[] prefixes, int valueCount) {
            this.words = words;
            this.prefixes = prefixes;
            this.valueCount = valueCount;
        }

        int get(int value) {
            Preconditions.checkArgument(
                    value >= 0 && value < valueCount, "Invalid FM quaternary offset.");
            return (int) ((words[value >>> 5] >>> ((value & 31) * 2)) & 3L);
        }

        int rank(int digit, int end) {
            Preconditions.checkArgument(
                    digit >= 0 && digit < 4 && end >= 0 && end <= valueCount,
                    "Invalid FM quaternary rank range.");
            int fullWords = end >>> 5;
            int group = fullWords >>> 6;
            int count = prefixes[group * 4 + digit];
            for (int i = group << 6; i < fullWords; i++) {
                count += countDigit(words[i], digit, QUAD_VALUES_PER_WORD);
            }
            int remaining = end & (QUAD_VALUES_PER_WORD - 1);
            if (remaining > 0) {
                count += countDigit(words[fullWords], digit, remaining);
            }
            return count;
        }

        int retainedSizeInBytes() {
            return words.length * Long.BYTES + prefixes.length * Integer.BYTES;
        }
    }

    static final class BitBlock {
        private final long[] words;
        private final int[] prefixes;
        private final int bitCount;

        BitBlock(long[] words, int[] prefixes, int bitCount) {
            this.words = words;
            this.prefixes = prefixes;
            this.bitCount = bitCount;
        }

        boolean get(int bit) {
            Preconditions.checkArgument(bit >= 0 && bit < bitCount, "Invalid FM bit offset.");
            return (words[bit >>> 6] & (1L << (bit & 63))) != 0;
        }

        int rankOnes(int end) {
            Preconditions.checkArgument(end >= 0 && end <= bitCount, "Invalid FM rank range.");
            int fullWords = end >>> 6;
            int group = fullWords >>> 6;
            int ones = prefixes[group];
            for (int i = group << 6; i < fullWords; i++) {
                ones += Long.bitCount(words[i]);
            }
            int remaining = end & 63;
            if (remaining > 0) {
                ones += Long.bitCount(words[fullWords] & ((1L << remaining) - 1L));
            }
            return ones;
        }

        int retainedSizeInBytes() {
            return words.length * Long.BYTES + prefixes.length * Integer.BYTES;
        }
    }
}
