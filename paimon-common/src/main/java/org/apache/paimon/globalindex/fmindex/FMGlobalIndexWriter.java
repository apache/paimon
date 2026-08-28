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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Streaming, bounded-partition writer for an exact byte-oriented FM index. */
public class FMGlobalIndexWriter implements GlobalIndexSingleColumnWriter, Closeable {

    private static final int VERIFICATION_PAGE_ROW_COUNT = 128;
    private static final int TARGET_VERIFICATION_PAGE_SIZE = 64 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final int maxPartitionTextLength;
    private final int maxPartitionRowCount;
    private final int sampleRate;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final List<ResultEntry> results = new ArrayList<>();

    private CharBuilder text = new CharBuilder();
    private boolean[] nullRows = new boolean[128];
    private long partitionFirstRowId;
    private int partitionRowCount;
    private long lastRowId;
    private boolean hasLastRowId;
    private boolean finished;

    FMGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            int maxPartitionTextLength,
            int maxPartitionRowCount,
            int sampleRate,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.fileWriter = fileWriter;
        this.maxPartitionTextLength = maxPartitionTextLength;
        this.maxPartitionRowCount = maxPartitionRowCount;
        this.sampleRate = sampleRate;
        this.compressionFactory = compressionFactory;
    }

    @Override
    public void write(@Nullable Object key, long relativeRowId) {
        Preconditions.checkState(!finished, "Cannot write after the FM index writer is finished.");
        Preconditions.checkArgument(relativeRowId >= 0, "FM index row ID must be non-negative.");
        Preconditions.checkArgument(
                !hasLastRowId || relativeRowId == lastRowId + 1,
                "FM index row IDs must be consecutive: previous=%s, current=%s.",
                lastRowId,
                relativeRowId);

        byte[] bytes = null;
        if (key != null) {
            Preconditions.checkArgument(
                    key instanceof BinaryString,
                    "FM index expects BinaryString values, but found %s.",
                    key.getClass().getName());
            bytes = ((BinaryString) key).toBytes();
            Preconditions.checkArgument(
                    bytes.length
                            <= FMIndexFile.MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH
                                    - Integer.BYTES,
                    "A value exceeds the FM index exact-fallback block limit (%s bytes).",
                    FMIndexFile.MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH);
        }
        long encodedLength = (bytes == null ? 0L : bytes.length) + 1L;
        Preconditions.checkArgument(
                encodedLength < maxPartitionTextLength,
                "A value exceeds fm-index.partition-size (%s encoded symbols).",
                maxPartitionTextLength);
        if (partitionRowCount > 0
                && (partitionRowCount >= maxPartitionRowCount
                        || text.size() > maxPartitionTextLength - encodedLength - 1)) {
            flushPartition();
        }

        if (partitionRowCount == 0) {
            partitionFirstRowId = relativeRowId;
        }
        ensureNullCapacity(partitionRowCount + 1);
        nullRows[partitionRowCount] = bytes == null;
        if (bytes != null) {
            for (byte value : bytes) {
                text.add((value & 0xFF) + FMIndexFile.FIRST_BYTE_SYMBOL);
            }
        }
        text.add(FMIndexFile.SEPARATOR);
        partitionRowCount++;
        lastRowId = relativeRowId;
        hasLastRowId = true;
    }

    @Override
    public List<ResultEntry> finish() {
        Preconditions.checkState(!finished, "FM index writer is already finished.");
        finished = true;
        flushPartition();
        return new ArrayList<>(results);
    }

    @Override
    public void close() {
        finished = true;
        text = new CharBuilder();
        partitionRowCount = 0;
    }

    private void flushPartition() {
        if (partitionRowCount == 0) {
            return;
        }
        try {
            text.add(FMIndexFile.TERMINATOR);
            char[] symbols = text.toArray();
            text = new CharBuilder();
            DenseAlphabet alphabet = densify(symbols);
            int[] suffixArray = FMIndexSuffixArray.build(symbols, alphabet.alphabetSize - 1);
            short[] bwt = new short[symbols.length];
            long[] sampledWords = new long[wordsForBits(symbols.length)];
            int sampleCount = ((symbols.length - 1) / sampleRate) + 1;
            int[] sampleValues = new int[sampleCount];
            int samplePosition = 0;
            for (int row = 0; row < suffixArray.length; row++) {
                int suffix = suffixArray[row];
                bwt[row] = (short) symbols[suffix == 0 ? symbols.length - 1 : suffix - 1];
                if (suffix % sampleRate == 0) {
                    sampledWords[row >>> 6] |= 1L << (row & 63);
                    sampleValues[samplePosition++] = suffix;
                }
            }
            Preconditions.checkState(
                    samplePosition == sampleValues.length,
                    "FM index sampled suffix count is inconsistent.");
            suffixArray = null;

            int[] cumulative = cumulativeCounts(symbols, alphabet.alphabetSize);
            int levelCount = FMIndexFile.levelsForAlphabet(alphabet.alphabetSize);
            int[][] digitStarts = new int[levelCount][4];
            FMIndexFile.QuadVectorMeta[] wavelets = new FMIndexFile.QuadVectorMeta[levelCount];
            String fileName = fileWriter.newFileName("fmindex");
            try (PositionOutputStream stream = fileWriter.newOutputStream(fileName)) {
                DataOutputStream output = new DataOutputStream(stream);
                short[] current = bwt;
                short[] reordered = new short[bwt.length];
                for (int level = 0; level < levelCount; level++) {
                    int shift = (levelCount - level - 1) * 2;
                    long[] quads = new long[FMIndexFile.wordsForQuads(current.length)];
                    int[] counts = new int[4];
                    for (int i = 0; i < current.length; i++) {
                        int digit = ((current[i] & 0xFFFF) >>> shift) & 3;
                        counts[digit]++;
                        quads[i >>> 5] |= (long) digit << ((i & 31) * 2);
                    }
                    int next = 0;
                    for (int digit = 0; digit < 4; digit++) {
                        digitStarts[level][digit] = next;
                        next += counts[digit];
                    }
                    int[] positions = java.util.Arrays.copyOf(digitStarts[level], 4);
                    for (short encoded : current) {
                        int symbol = encoded & 0xFFFF;
                        int digit = (symbol >>> shift) & 3;
                        reordered[positions[digit]++] = encoded;
                    }
                    wavelets[level] =
                            FMIndexFile.writeQuadVector(
                                    stream, output, quads, current.length, compressionFactory);
                    short[] swap = current;
                    current = reordered;
                    reordered = swap;
                }

                FMIndexFile.BitVectorMeta sampled =
                        FMIndexFile.writeBitVector(
                                stream, output, sampledWords, symbols.length, compressionFactory);
                FMIndexFile.IntVectorMeta samples =
                        FMIndexFile.writeIntVector(
                                stream, output, sampleValues, compressionFactory);
                long[] nullWords = new long[wordsForBits(partitionRowCount)];
                for (int i = 0; i < partitionRowCount; i++) {
                    if (nullRows[i]) {
                        nullWords[i >>> 6] |= 1L << (i & 63);
                    }
                }
                FMIndexFile.BitVectorMeta nullVector =
                        FMIndexFile.writeBitVector(
                                stream, output, nullWords, partitionRowCount, compressionFactory);
                long[] boundaryWords = new long[wordsForBits(symbols.length)];
                for (int i = 0; i < symbols.length; i++) {
                    if (symbols[i] == FMIndexFile.SEPARATOR) {
                        boundaryWords[i >>> 6] |= 1L << (i & 63);
                    }
                }
                FMIndexFile.BitVectorMeta rowBoundaries =
                        FMIndexFile.writeBitVector(
                                stream, output, boundaryWords, symbols.length, compressionFactory);
                List<FMIndexFile.VerificationPageMeta> verificationPages =
                        writeVerificationPages(stream, output, symbols, alphabet.symbolToByte);
                FMIndexFile.Directory directory =
                        new FMIndexFile.Directory(
                                partitionRowCount,
                                symbols.length,
                                sampleRate,
                                levelCount,
                                alphabet.alphabetSize,
                                alphabet.byteToSymbol,
                                cumulative,
                                digitStarts,
                                wavelets,
                                sampled,
                                samples,
                                nullVector,
                                rowBoundaries,
                                verificationPages);
                FMIndexFile.BlockInfo directoryBlock =
                        FMIndexFile.writeDirectory(stream, output, directory, compressionFactory);
                FMIndexFile.writeFooter(
                        output,
                        directoryBlock,
                        partitionFirstRowId,
                        partitionRowCount,
                        symbols.length,
                        sampleRate);
            }
            results.add(
                    new ResultEntry(
                            fileName,
                            partitionRowCount,
                            FMIndexFile.writeIndexMeta(partitionFirstRowId, partitionRowCount)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write FM global index.", e);
        } finally {
            text = new CharBuilder();
            Arrays.fill(nullRows, 0, partitionRowCount, false);
            partitionRowCount = 0;
        }
    }

    private static int[] cumulativeCounts(char[] symbols, int alphabetSize) {
        int[] counts = new int[alphabetSize + 1];
        for (char symbol : symbols) {
            counts[symbol + 1]++;
        }
        for (int i = 1; i < counts.length; i++) {
            counts[i] += counts[i - 1];
        }
        return counts;
    }

    private List<FMIndexFile.VerificationPageMeta> writeVerificationPages(
            PositionOutputStream stream,
            DataOutputStream output,
            char[] symbols,
            int[] symbolToByte)
            throws IOException {
        List<FMIndexFile.VerificationPageMeta> pages = new ArrayList<>();
        int row = 0;
        int symbolPosition = 0;
        while (row < partitionRowCount) {
            int firstRow = row;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream values = new DataOutputStream(bytes);
            int pageRows = 0;
            while (row < partitionRowCount) {
                int separator = symbolPosition;
                while (separator < symbols.length && symbols[separator] != FMIndexFile.SEPARATOR) {
                    separator++;
                }
                Preconditions.checkState(
                        separator < symbols.length,
                        "FM index encoded text is missing a row separator.");
                int valueLength = separator - symbolPosition;
                long recordLength = Integer.BYTES + (nullRows[row] ? 0L : valueLength);
                if (pageRows > 0
                        && (pageRows >= VERIFICATION_PAGE_ROW_COUNT
                                || bytes.size() + recordLength > TARGET_VERIFICATION_PAGE_SIZE)) {
                    break;
                }
                Preconditions.checkState(
                        bytes.size() + recordLength
                                <= FMIndexFile.MAX_VERIFICATION_BLOCK_UNCOMPRESSED_LENGTH,
                        "FM index verification value exceeds the supported block size.");
                if (nullRows[row]) {
                    Preconditions.checkState(
                            valueLength == 0, "FM index null row contains encoded bytes.");
                    values.writeInt(-1);
                } else {
                    values.writeInt(valueLength);
                    for (int i = symbolPosition; i < separator; i++) {
                        values.writeByte(symbolToByte[symbols[i]]);
                    }
                }
                symbolPosition = separator + 1;
                row++;
                pageRows++;
            }
            values.flush();
            pages.add(
                    new FMIndexFile.VerificationPageMeta(
                            firstRow,
                            pageRows,
                            FMIndexFile.writeBlock(
                                    stream, output, bytes.toByteArray(), compressionFactory)));
        }
        Preconditions.checkState(
                symbolPosition == symbols.length - 1
                        && symbols[symbolPosition] == FMIndexFile.TERMINATOR,
                "FM index verification rows do not cover the encoded text.");
        return pages;
    }

    private static DenseAlphabet densify(char[] symbols) {
        boolean[] present = new boolean[256];
        for (char symbol : symbols) {
            if (symbol >= FMIndexFile.FIRST_BYTE_SYMBOL) {
                present[symbol - FMIndexFile.FIRST_BYTE_SYMBOL] = true;
            }
        }
        int[] byteToSymbol = new int[256];
        Arrays.fill(byteToSymbol, -1);
        int alphabetSize = FMIndexFile.FIRST_BYTE_SYMBOL;
        for (int value = 0; value < present.length; value++) {
            if (present[value]) {
                byteToSymbol[value] = alphabetSize++;
            }
        }
        int[] symbolToByte = new int[alphabetSize];
        for (int value = 0; value < byteToSymbol.length; value++) {
            if (byteToSymbol[value] >= 0) {
                symbolToByte[byteToSymbol[value]] = value;
            }
        }
        for (int i = 0; i < symbols.length; i++) {
            if (symbols[i] >= FMIndexFile.FIRST_BYTE_SYMBOL) {
                symbols[i] = (char) byteToSymbol[symbols[i] - FMIndexFile.FIRST_BYTE_SYMBOL];
            }
        }
        return new DenseAlphabet(alphabetSize, byteToSymbol, symbolToByte);
    }

    private void ensureNullCapacity(int capacity) {
        if (capacity <= nullRows.length) {
            return;
        }
        int next = Math.max(capacity, nullRows.length + (nullRows.length >>> 1));
        nullRows = Arrays.copyOf(nullRows, next);
    }

    private static int wordsForBits(int bitLength) {
        return (int) (((long) bitLength + Long.SIZE - 1) / Long.SIZE);
    }

    private static final class DenseAlphabet {
        private final int alphabetSize;
        private final int[] byteToSymbol;
        private final int[] symbolToByte;

        private DenseAlphabet(int alphabetSize, int[] byteToSymbol, int[] symbolToByte) {
            this.alphabetSize = alphabetSize;
            this.byteToSymbol = byteToSymbol;
            this.symbolToByte = symbolToByte;
        }
    }

    private static final class CharBuilder {
        private char[] values = new char[1024];
        private int size;

        void add(int value) {
            Preconditions.checkArgument(
                    value >= Character.MIN_VALUE && value <= Character.MAX_VALUE,
                    "FM index symbol is outside the compact symbol range.");
            if (size == values.length) {
                int next = values.length + (values.length >>> 1);
                values = Arrays.copyOf(values, next);
            }
            values[size++] = (char) value;
        }

        int size() {
            return size;
        }

        char[] toArray() {
            return Arrays.copyOf(values, size);
        }
    }
}
