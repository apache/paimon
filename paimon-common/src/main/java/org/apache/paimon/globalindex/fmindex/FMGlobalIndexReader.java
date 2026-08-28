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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.globalindex.ContainsRefiningGlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.ToIntFunction;

/** Exact FM-index reader using blocked wavelet ranks and value-sampled suffix locations. */
final class FMGlobalIndexReader implements ContainsRefiningGlobalIndexReader {

    private static final int MAX_VERIFICATION_RANGE_SIZE = 4 * 1024 * 1024;
    private static final int MAX_VERIFICATION_UNCOMPRESSED_RANGE_SIZE = 4 * 1024 * 1024;
    private static final int MAX_VERIFICATION_RANGE_BATCH_SIZE = 8;
    private static final int ESTIMATED_READ_REQUEST_BYTES = 64 * 1024;

    @Nullable private final GlobalIndexFileReader fileReader;
    @Nullable private final GlobalIndexIOMeta file;
    private final ExecutorService executor;
    private final FMIndexReadContext readContext;
    @Nullable private final FileSetRowCountValidator rowCountValidator;
    @Nullable private final FMIndexFile.IndexMeta indexMeta;
    private final int filePosition;
    private final int demandPageSize;
    private final double locateCostRatio;

    @Nullable private volatile Metadata metadata;

    FMGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta file,
            ExecutorService executor,
            FMIndexReadContext readContext,
            FileSetRowCountValidator rowCountValidator,
            @Nullable FMIndexFile.IndexMeta indexMeta,
            int filePosition,
            int demandPageSize,
            double locateCostRatio) {
        this.fileReader = fileReader;
        this.file = file;
        this.executor = executor;
        this.readContext = readContext;
        this.rowCountValidator = rowCountValidator;
        this.indexMeta = indexMeta;
        this.filePosition = filePosition;
        this.demandPageSize = readContext.effectiveDemandPageSize(demandPageSize);
        this.locateCostRatio = locateCostRatio;
    }

    private FMGlobalIndexReader(
            ExecutorService executor,
            FMIndexReadContext readContext,
            int demandPageSize,
            double locateCostRatio) {
        this.fileReader = null;
        this.file = null;
        this.executor = executor;
        this.readContext = readContext;
        this.rowCountValidator = null;
        this.indexMeta = null;
        this.filePosition = -1;
        this.demandPageSize = readContext.effectiveDemandPageSize(demandPageSize);
        this.locateCostRatio = locateCostRatio;
    }

    static FMGlobalIndexReader empty(
            ExecutorService executor,
            FMIndexReadContext readContext,
            int demandPageSize,
            double locateCostRatio) {
        return new FMGlobalIndexReader(executor, readContext, demandPageSize, locateCostRatio);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        byte[] needle = needle(literal);
        if (needle == null || file == null) {
            return exactEmpty();
        }
        return queryAsync(Collections.singletonList(needle), null);
    }

    /** FM has no lossy coarse phase; returning empty avoids enumerating the same interval twice. */
    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContainsCandidates(
            FieldRef fieldRef, List<Object> literals, @Nullable GlobalIndexResult candidates) {
        Preconditions.checkArgument(!literals.isEmpty(), "Contains candidates must not be empty.");
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContainsConjunction(
            FieldRef fieldRef, List<Object> literals, @Nullable GlobalIndexResult candidates) {
        Preconditions.checkArgument(!literals.isEmpty(), "Contains conjunction must not be empty.");
        if (file == null || (candidates != null && candidates.results().isEmpty())) {
            return exactEmpty();
        }
        List<byte[]> needles = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            byte[] needle = needle(literal);
            if (needle == null) {
                return exactEmpty();
            }
            needles.add(needle);
        }
        return queryAsync(needles, candidates);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return file == null ? exactEmpty() : nullQueryAsync(true);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return file == null ? exactEmpty() : nullQueryAsync(false);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> queryAsync(
            List<byte[]> needles, @Nullable GlobalIndexResult candidates) {
        if (candidatesDisjointFromIndexMeta(candidates)) {
            return exactEmpty();
        }
        return CompletableFuture.supplyAsync(
                () -> readContext.withFileReadPermit(() -> queryUnchecked(needles, candidates)),
                executor);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> nullQueryAsync(boolean nulls) {
        return CompletableFuture.supplyAsync(
                () -> readContext.withFileReadPermit(() -> queryNullsUnchecked(nulls)), executor);
    }

    private Optional<GlobalIndexResult> queryUnchecked(
            List<byte[]> needles, @Nullable GlobalIndexResult candidates) {
        if (candidatesDisjointFromIndexMeta(candidates)) {
            return exactEmptyResult();
        }
        try (SeekableInputStream input = inputStream()) {
            Metadata current = metadata(input);
            if (candidates != null
                    && !candidates
                            .results()
                            .intersects(
                                    current.footer.firstRowId,
                                    current.footer.firstRowId + current.footer.rowCount)) {
                return exactEmptyResult();
            }
            // Enumerate selective intervals and use the stored-value path when occurrence-level
            // SA location would cost more than Milvus' count-first guard permits.
            List<SearchInterval> intervals = new ArrayList<>(needles.size());
            for (byte[] needle : needles) {
                if (needle.length > 0) {
                    SearchInterval interval = backwardSearch(input, current.directory, needle);
                    if (interval.isEmpty()) {
                        return exactEmptyResult();
                    }
                    intervals.add(interval);
                }
            }
            intervals.sort((left, right) -> Integer.compare(left.size(), right.size()));

            RoaringNavigableMap64 result = null;
            List<byte[]> denseNeedles = new ArrayList<>();
            for (SearchInterval interval : intervals) {
                RoaringNavigableMap64 effectiveCandidates =
                        result != null ? result : candidates == null ? null : candidates.results();
                if (!shouldLocate(current, interval, effectiveCandidates)) {
                    denseNeedles.add(interval.needle);
                    continue;
                }
                RoaringNavigableMap64 matches = locateRows(input, current, interval);
                if (result == null) {
                    result = matches;
                    if (candidates != null) {
                        result.and(candidates.results());
                    }
                } else {
                    result.and(matches);
                }
                if (result.isEmpty()) {
                    return exactEmptyResult();
                }
            }
            if (!denseNeedles.isEmpty()) {
                RoaringNavigableMap64 verificationCandidates =
                        result != null ? result : candidates == null ? null : candidates.results();
                result = verifyRows(input, current, denseNeedles, verificationCandidates);
            } else if (result == null) {
                result = nullRows(input, current, false);
                if (candidates != null) {
                    result.and(candidates.results());
                }
            }
            return Optional.of(GlobalIndexResult.create(result));
        } catch (IOException e) {
            throw new CompletionException("Failed to read FM global index.", e);
        }
    }

    private boolean candidatesDisjointFromIndexMeta(@Nullable GlobalIndexResult candidates) {
        return candidates != null
                && indexMeta != null
                && !candidates
                        .results()
                        .intersects(
                                indexMeta.firstRowId, indexMeta.firstRowId + indexMeta.rowCount);
    }

    private Optional<GlobalIndexResult> queryNullsUnchecked(boolean nulls) {
        try (SeekableInputStream input = inputStream()) {
            Metadata current = metadata(input);
            return Optional.of(GlobalIndexResult.create(nullRows(input, current, nulls)));
        } catch (IOException e) {
            throw new CompletionException("Failed to read FM global index.", e);
        }
    }

    private SearchInterval backwardSearch(
            SeekableInputStream input, FMIndexFile.Directory directory, byte[] needle)
            throws IOException {
        int lower = 0;
        int upper = directory.textLength;
        for (int i = needle.length - 1; i >= 0 && lower < upper; i--) {
            int symbol = directory.byteToSymbol[needle[i] & 0xFF];
            if (symbol < 0) {
                return new SearchInterval(0, 0, needle);
            }
            int cumulative = directory.cumulativeCounts[symbol];
            RankPair ranks = rankPair(input, directory, symbol, lower, upper);
            lower = cumulative + ranks.lower;
            upper = cumulative + ranks.upper;
        }
        return new SearchInterval(lower, upper, needle);
    }

    private boolean shouldLocate(
            Metadata metadata,
            SearchInterval interval,
            @Nullable RoaringNavigableMap64 candidates) {
        FMIndexFile.Directory directory = metadata.directory;
        double locateCost = (double) interval.size() * directory.sampleRate;
        if (candidates != null) {
            long verificationBytes = 0;
            for (FMIndexFile.VerificationPageMeta page : directory.verificationPages) {
                if (pageSelected(metadata.footer.firstRowId, page, candidates)) {
                    verificationBytes += page.block.uncompressedLength;
                }
            }
            if (locateCost >= locateCostRatio * verificationBytes) {
                return false;
            }
        }
        long textBytes = (long) directory.textLength - directory.rowCount - 1;
        return locateCost < locateCostRatio * textBytes;
    }

    private RoaringNavigableMap64 verifyRows(
            SeekableInputStream input,
            Metadata metadata,
            List<byte[]> needles,
            @Nullable RoaringNavigableMap64 candidates)
            throws IOException {
        List<FMBytePattern> patterns = new ArrayList<>(needles.size());
        for (byte[] needle : needles) {
            patterns.add(new FMBytePattern(needle));
        }
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        List<VerificationRange> ranges = verificationRanges(metadata, candidates);
        for (int rangePosition = 0; rangePosition < ranges.size(); ) {
            int rangeEnd = rangePosition;
            long batchStoredBytes = 0;
            while (rangeEnd < ranges.size()
                    && rangeEnd - rangePosition < MAX_VERIFICATION_RANGE_BATCH_SIZE) {
                VerificationRange range = ranges.get(rangeEnd);
                if (rangeEnd > rangePosition
                        && batchStoredBytes + range.storedLength > MAX_VERIFICATION_RANGE_SIZE) {
                    break;
                }
                batchStoredBytes += range.storedLength;
                rangeEnd++;
            }
            List<VerificationRange> batch = ranges.subList(rangePosition, rangeEnd);
            byte[][] storedRanges = readVerificationRanges(input, batch);
            for (int rangeIndex = 0; rangeIndex < batch.size(); rangeIndex++) {
                VerificationRange range = batch.get(rangeIndex);
                List<byte[]> pages =
                        FMIndexFile.decodeVerificationBlockRange(
                                storedRanges[rangeIndex], range.blocks);
                for (int pageOffset = 0; pageOffset < pages.size(); pageOffset++) {
                    verifyPage(
                            metadata.footer.firstRowId,
                            metadata.directory.verificationPages.get(range.pageStart + pageOffset),
                            pages.get(pageOffset),
                            patterns,
                            candidates,
                            result);
                }
            }
            rangePosition = rangeEnd;
        }
        return result;
    }

    private List<VerificationRange> verificationRanges(
            Metadata metadata, @Nullable RoaringNavigableMap64 candidates) {
        List<FMIndexFile.VerificationPageMeta> pages = metadata.directory.verificationPages;
        List<VerificationRange> ranges = new ArrayList<>();
        int pagePosition = 0;
        while (pagePosition < pages.size()) {
            if (!pageSelected(metadata.footer.firstRowId, pages.get(pagePosition), candidates)) {
                pagePosition++;
                continue;
            }
            int pageStart = pagePosition;
            List<FMIndexFile.BlockInfo> blocks = new ArrayList<>();
            long storedBytes = 0;
            long uncompressedBytes = 0;
            long physicalEnd = -1;
            while (pagePosition < pages.size()
                    && pageSelected(
                            metadata.footer.firstRowId, pages.get(pagePosition), candidates)) {
                FMIndexFile.BlockInfo block = pages.get(pagePosition).block;
                if (!blocks.isEmpty()
                        && (block.offset != physicalEnd
                                || storedBytes + block.storedLength > MAX_VERIFICATION_RANGE_SIZE
                                || uncompressedBytes + block.uncompressedLength
                                        > MAX_VERIFICATION_UNCOMPRESSED_RANGE_SIZE)) {
                    break;
                }
                FMIndexFile.validateVerificationBlock(block, fileSize());
                blocks.add(block);
                storedBytes += block.storedLength;
                uncompressedBytes += block.uncompressedLength;
                physicalEnd = block.offset + block.storedLength;
                pagePosition++;
            }
            ranges.add(new VerificationRange(pageStart, blocks, storedBytes));
        }
        return ranges;
    }

    private byte[][] readVerificationRanges(
            SeekableInputStream input, List<VerificationRange> ranges) throws IOException {
        byte[][] result = new byte[ranges.size()][];
        if (ranges.size() == 1 || !(input instanceof VectoredReadable)) {
            for (int i = 0; i < ranges.size(); i++) {
                result[i] =
                        FMIndexFile.readVerificationBlockRange(
                                input, ranges.get(i).blocks, fileSize());
            }
            return result;
        }

        VectoredReadable readable = (VectoredReadable) input;
        List<FileRange> fileRanges = new ArrayList<>(ranges.size());
        for (VerificationRange range : ranges) {
            fileRanges.add(
                    FileRange.createFileRange(
                            range.blocks.get(0).offset, (int) range.storedLength));
        }
        VectoredReadUtils.ReadOptions options =
                VectoredReadUtils.ReadOptions.from(readable)
                        .withMinSeekForVectorReads(ESTIMATED_READ_REQUEST_BYTES)
                        .withSequentialReadFallback(false);
        VectoredReadUtils.readVectored(readable, fileRanges, options);
        for (int i = 0; i < fileRanges.size(); i++) {
            try {
                result[i] = fileRanges.get(i).getData().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw e;
            }
        }
        return result;
    }

    private static void verifyPage(
            long firstRowId,
            FMIndexFile.VerificationPageMeta page,
            byte[] values,
            List<FMBytePattern> patterns,
            @Nullable RoaringNavigableMap64 candidates,
            RoaringNavigableMap64 result) {
        int offset = 0;
        for (int row = 0; row < page.rowCount; row++) {
            Preconditions.checkState(
                    offset <= values.length - Integer.BYTES,
                    "FM index verification page ended before its declared rows.");
            int length = readInt(values, offset);
            offset += Integer.BYTES;
            Preconditions.checkState(
                    length == -1 || (length >= 0 && length <= values.length - offset),
                    "Invalid FM index verification value length.");
            long rowId = firstRowId + page.firstRow + row;
            if (length >= 0
                    && (candidates == null || candidates.contains(rowId))
                    && matchesAll(patterns, values, offset, length)) {
                result.add(rowId);
            }
            if (length >= 0) {
                offset += length;
            }
        }
        Preconditions.checkState(
                offset == values.length, "FM index verification page has trailing bytes.");
    }

    private static boolean pageSelected(
            long firstRowId,
            FMIndexFile.VerificationPageMeta page,
            @Nullable RoaringNavigableMap64 candidates) {
        if (candidates == null) {
            return true;
        }
        long first = firstRowId + page.firstRow;
        return candidates.intersects(first, first + page.rowCount);
    }

    private static boolean matchesAll(
            List<FMBytePattern> patterns, byte[] value, int offset, int length) {
        for (FMBytePattern pattern : patterns) {
            if (!pattern.contains(value, offset, length)) {
                return false;
            }
        }
        return true;
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private RoaringNavigableMap64 locateRows(
            SeekableInputStream input, Metadata metadata, SearchInterval interval)
            throws IOException {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (int bwtRow = interval.lower; bwtRow < interval.upper; bwtRow++) {
            int textPosition = locate(input, metadata.directory, bwtRow);
            Preconditions.checkState(
                    textPosition >= 0 && textPosition < metadata.directory.textLength - 1,
                    "FM index located an invalid text position.");
            int rowOrdinal = rankOnes(input, metadata.directory.rowBoundaries, textPosition);
            Preconditions.checkState(
                    rowOrdinal >= 0 && rowOrdinal < metadata.directory.rowCount,
                    "FM index located an invalid row ordinal: bwtRow=%s, textPosition=%s, rowOrdinal=%s, rowCount=%s.",
                    bwtRow,
                    textPosition,
                    rowOrdinal,
                    metadata.directory.rowCount);
            result.add(metadata.footer.firstRowId + rowOrdinal);
        }
        return result;
    }

    private int locate(SeekableInputStream input, FMIndexFile.Directory directory, int bwtRow)
            throws IOException {
        int current = bwtRow;
        int steps = 0;
        while (!bit(input, directory.sampledRows, current)) {
            current = lf(input, directory, current);
            steps++;
            Preconditions.checkState(
                    steps < directory.sampleRate,
                    "FM index SA locate exceeded its value-sampling bound.");
        }
        int sampleOrdinal = rankOnes(input, directory.sampledRows, current);
        int sample = sampleValue(input, directory.sampleValues, sampleOrdinal);
        Preconditions.checkState(
                sample >= 0 && sample < directory.textLength && sample % directory.sampleRate == 0,
                "Invalid FM index sampled suffix value.");
        return (int) (((long) sample + steps) % directory.textLength);
    }

    private int lf(SeekableInputStream input, FMIndexFile.Directory directory, int position)
            throws IOException {
        int current = position;
        int symbol = 0;
        for (int level = 0; level < directory.levelCount; level++) {
            FMIndexFile.QuadBlockMeta meta = directory.wavelets[level].block(current);
            FMIndexFile.QuadBlock block = quadBlock(input, directory.wavelets[level], meta);
            int local = current - meta.firstValue;
            int digit = block.get(local);
            symbol = (symbol << 2) | digit;
            current =
                    directory.digitStarts[level][digit]
                            + meta.prefixCounts[digit]
                            + block.rank(digit, local);
        }
        Preconditions.checkState(
                symbol < directory.alphabetSize
                        && current >= directory.waveletStarts[symbol]
                        && current
                                < directory.waveletStarts[symbol]
                                        + directory.cumulativeCounts[symbol + 1]
                                        - directory.cumulativeCounts[symbol],
                "FM index LF mapping returned an invalid row.");
        return directory.cumulativeCounts[symbol] + current - directory.waveletStarts[symbol];
    }

    private RankPair rankPair(
            SeekableInputStream input,
            FMIndexFile.Directory directory,
            int symbol,
            int lower,
            int upper)
            throws IOException {
        Preconditions.checkArgument(
                lower >= 0 && lower <= upper && upper <= directory.textLength,
                "Invalid FM rank interval.");
        for (int level = 0; level < directory.levelCount; level++) {
            int shift = (directory.levelCount - level - 1) * 2;
            int digit = (symbol >>> shift) & 3;
            int start = directory.digitStarts[level][digit];
            RankPair ranks = rankDigitPair(input, directory.wavelets[level], digit, lower, upper);
            lower = start + ranks.lower;
            upper = start + ranks.upper;
        }
        int symbolStart = directory.waveletStarts[symbol];
        return new RankPair(lower - symbolStart, upper - symbolStart);
    }

    private RankPair rankDigitPair(
            SeekableInputStream input,
            FMIndexFile.QuadVectorMeta vector,
            int digit,
            int lower,
            int upper)
            throws IOException {
        if (lower == upper) {
            int rank = rankDigit(input, vector, digit, lower);
            return new RankPair(rank, rank);
        }
        FMIndexFile.QuadBlockMeta lowerMeta = rankBlock(vector, lower);
        FMIndexFile.QuadBlockMeta upperMeta = rankBlock(vector, upper);
        if (lowerMeta != null && lowerMeta == upperMeta) {
            FMIndexFile.QuadBlock block = quadBlock(input, vector, lowerMeta);
            return new RankPair(
                    lowerMeta.prefixCounts[digit] + block.rank(digit, lower - lowerMeta.firstValue),
                    upperMeta.prefixCounts[digit]
                            + block.rank(digit, upper - upperMeta.firstValue));
        }
        return new RankPair(
                rankDigit(input, vector, digit, lower), rankDigit(input, vector, digit, upper));
    }

    @Nullable
    private static FMIndexFile.QuadBlockMeta rankBlock(FMIndexFile.QuadVectorMeta vector, int end) {
        return end == 0 || end == vector.valueLength || end % FMIndexFile.QUAD_BLOCK_VALUES == 0
                ? null
                : vector.block(end - 1);
    }

    private int rankDigit(
            SeekableInputStream input, FMIndexFile.QuadVectorMeta vector, int digit, int end)
            throws IOException {
        if (end == 0) {
            return 0;
        }
        if (end == vector.valueLength) {
            return vector.totalCounts[digit];
        }
        if (end % FMIndexFile.QUAD_BLOCK_VALUES == 0) {
            return vector.block(end).prefixCounts[digit];
        }
        FMIndexFile.QuadBlockMeta meta = vector.block(end - 1);
        return meta.prefixCounts[digit]
                + quadBlock(input, vector, meta).rank(digit, end - meta.firstValue);
    }

    private FMIndexFile.QuadBlock quadBlock(
            SeekableInputStream input,
            FMIndexFile.QuadVectorMeta vector,
            FMIndexFile.QuadBlockMeta meta)
            throws IOException {
        Preconditions.checkState(file != null, "Missing FM index file.");
        FMIndexFile.QuadBlock block =
                readContext.get(file, meta.block, FMIndexFile.QuadBlock.class);
        if (block != null) {
            return block;
        }
        int blockIndex = meta.firstValue / FMIndexFile.QUAD_BLOCK_VALUES;
        List<FMIndexFile.QuadBlockMeta> demandPage =
                demandPage(
                        vector.blocks,
                        blockIndex,
                        demandPageSize,
                        blockMeta -> blockMeta.block.uncompressedLength);
        List<FMIndexFile.BlockInfo> blockInfos = new ArrayList<>(demandPage.size());
        for (FMIndexFile.QuadBlockMeta pageBlock : demandPage) {
            blockInfos.add(pageBlock.block);
        }
        List<byte[]> pageBytes = FMIndexFile.readBlocks(input, blockInfos, file.fileSize());
        FMIndexFile.QuadBlock requested = null;
        for (int i = 0; i < demandPage.size(); i++) {
            FMIndexFile.QuadBlockMeta pageMeta = demandPage.get(i);
            FMIndexFile.QuadBlock decoded =
                    readContext.get(file, pageMeta.block, FMIndexFile.QuadBlock.class);
            if (decoded == null) {
                decoded = FMIndexFile.decodeQuadBlock(pageBytes.get(i), pageMeta);
                readContext.put(
                        file,
                        pageMeta.block,
                        FMIndexFile.QuadBlock.class,
                        decoded,
                        decoded.retainedSizeInBytes());
            }
            if (pageMeta == meta) {
                requested = decoded;
            }
        }
        Preconditions.checkState(requested != null, "FM demand page missed its requested block.");
        return requested;
    }

    private static <T> List<T> demandPage(
            List<T> blocks,
            int requestedBlock,
            int targetBytes,
            ToIntFunction<T> uncompressedSize) {
        Preconditions.checkElementIndex(requestedBlock, blocks.size(), "FM wavelet block");
        int start = 0;
        while (start < blocks.size()) {
            int end = start;
            long bytes = 0;
            while (end < blocks.size()) {
                int blockBytes = uncompressedSize.applyAsInt(blocks.get(end));
                if (end > start && bytes + blockBytes > targetBytes) {
                    break;
                }
                bytes += blockBytes;
                end++;
            }
            if (requestedBlock < end) {
                return blocks.subList(start, end);
            }
            start = end;
        }
        throw new IllegalStateException("FM wavelet block is outside its demand pages.");
    }

    private boolean bit(SeekableInputStream input, FMIndexFile.BitVectorMeta vector, int position)
            throws IOException {
        FMIndexFile.BitBlockMeta meta = vector.block(position);
        return bitBlock(input, vector, meta).get(position - meta.firstBit);
    }

    private int rankOnes(SeekableInputStream input, FMIndexFile.BitVectorMeta vector, int end)
            throws IOException {
        if (end == 0) {
            return 0;
        }
        if (end == vector.bitLength) {
            return vector.totalOnes;
        }
        if (end % FMIndexFile.BLOCK_BITS == 0) {
            return vector.block(end).prefixOnes;
        }
        FMIndexFile.BitBlockMeta meta = vector.block(end - 1);
        return meta.prefixOnes + bitBlock(input, vector, meta).rankOnes(end - meta.firstBit);
    }

    private FMIndexFile.BitBlock bitBlock(
            SeekableInputStream input,
            FMIndexFile.BitVectorMeta vector,
            FMIndexFile.BitBlockMeta meta)
            throws IOException {
        Preconditions.checkState(file != null, "Missing FM index file.");
        FMIndexFile.BitBlock block = readContext.get(file, meta.block, FMIndexFile.BitBlock.class);
        if (block != null) {
            return block;
        }
        int blockIndex = meta.firstBit / FMIndexFile.BLOCK_BITS;
        List<FMIndexFile.BitBlockMeta> demandPage =
                demandPage(
                        vector.blocks,
                        blockIndex,
                        demandPageSize,
                        blockMeta -> blockMeta.block.uncompressedLength);
        List<FMIndexFile.BlockInfo> blockInfos = new ArrayList<>(demandPage.size());
        for (FMIndexFile.BitBlockMeta pageBlock : demandPage) {
            blockInfos.add(pageBlock.block);
        }
        List<byte[]> pageBytes = FMIndexFile.readBlocks(input, blockInfos, file.fileSize());
        FMIndexFile.BitBlock requested = null;
        for (int i = 0; i < demandPage.size(); i++) {
            FMIndexFile.BitBlockMeta pageMeta = demandPage.get(i);
            FMIndexFile.BitBlock decoded =
                    readContext.get(file, pageMeta.block, FMIndexFile.BitBlock.class);
            if (decoded == null) {
                decoded = FMIndexFile.decodeBitBlock(pageBytes.get(i), pageMeta);
                readContext.put(
                        file,
                        pageMeta.block,
                        FMIndexFile.BitBlock.class,
                        decoded,
                        decoded.retainedSizeInBytes());
            }
            if (pageMeta == meta) {
                requested = decoded;
            }
        }
        Preconditions.checkState(requested != null, "FM demand page missed its requested block.");
        return requested;
    }

    private int sampleValue(
            SeekableInputStream input, FMIndexFile.IntVectorMeta vector, int position)
            throws IOException {
        Preconditions.checkState(file != null, "Missing FM index file.");
        FMIndexFile.IntBlockMeta meta = vector.block(position);
        int[] values = readContext.get(file, meta.block, int[].class);
        if (values == null) {
            int blockIndex = meta.firstValue / FMIndexFile.VALUE_BLOCK_INTS;
            List<FMIndexFile.IntBlockMeta> demandPage =
                    demandPage(
                            vector.blocks,
                            blockIndex,
                            demandPageSize,
                            blockMeta -> blockMeta.block.uncompressedLength);
            List<FMIndexFile.BlockInfo> blockInfos = new ArrayList<>(demandPage.size());
            for (FMIndexFile.IntBlockMeta pageBlock : demandPage) {
                blockInfos.add(pageBlock.block);
            }
            List<byte[]> pageBytes = FMIndexFile.readBlocks(input, blockInfos, file.fileSize());
            for (int i = 0; i < demandPage.size(); i++) {
                FMIndexFile.IntBlockMeta pageMeta = demandPage.get(i);
                int[] decoded = readContext.get(file, pageMeta.block, int[].class);
                if (decoded == null) {
                    decoded = FMIndexFile.decodeIntBlock(pageBytes.get(i), pageMeta);
                    readContext.put(
                            file,
                            pageMeta.block,
                            int[].class,
                            decoded,
                            decoded.length * Integer.BYTES);
                }
                if (pageMeta == meta) {
                    values = decoded;
                }
            }
            Preconditions.checkState(
                    values != null, "FM demand page missed its requested sample block.");
        }
        return values[position - meta.firstValue];
    }

    private RoaringNavigableMap64 nullRows(
            SeekableInputStream input, Metadata metadata, boolean selectNulls) throws IOException {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (int row = 0; row < metadata.directory.rowCount; row++) {
            if (bit(input, metadata.directory.nullRows, row) == selectNulls) {
                result.add(metadata.footer.firstRowId + row);
            }
        }
        return result;
    }

    private Metadata metadata(SeekableInputStream input) throws IOException {
        Metadata current = metadata;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = metadata;
            if (current == null) {
                Preconditions.checkState(file != null, "Missing FM index file.");
                FMIndexFile.Footer footer = FMIndexFile.readFooter(input, file.fileSize());
                FMIndexFile.Directory directory =
                        FMIndexFile.readDirectory(input, footer, file.fileSize());
                Preconditions.checkState(
                        rowCountValidator != null, "Missing FM index row-count validator.");
                rowCountValidator.validate(
                        filePosition,
                        footer.rowCount,
                        footer.firstRowId,
                        footer.firstRowId + footer.rowCount - 1L);
                current = new Metadata(footer, directory);
                metadata = current;
            }
        }
        return current;
    }

    private SeekableInputStream inputStream() throws IOException {
        Preconditions.checkState(fileReader != null && file != null, "Missing FM index file.");
        return fileReader.getInputStream(file);
    }

    private long fileSize() {
        Preconditions.checkState(file != null, "Missing FM index file.");
        return file.fileSize();
    }

    @Nullable
    private static byte[] needle(@Nullable Object literal) {
        if (literal == null) {
            return null;
        }
        Preconditions.checkArgument(
                literal instanceof BinaryString,
                "FM contains literal must be BinaryString, but found %s.",
                literal.getClass().getName());
        return ((BinaryString) literal).toBytes();
    }

    private CompletableFuture<Optional<GlobalIndexResult>> exactEmpty() {
        return CompletableFuture.completedFuture(exactEmptyResult());
    }

    private static Optional<GlobalIndexResult> exactEmptyResult() {
        return Optional.of(GlobalIndexResult.createEmpty());
    }

    private static CompletableFuture<Optional<GlobalIndexResult>> unsupported() {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public void close() {}

    private static final class VerificationRange {
        private final int pageStart;
        private final List<FMIndexFile.BlockInfo> blocks;
        private final long storedLength;

        private VerificationRange(
                int pageStart, List<FMIndexFile.BlockInfo> blocks, long storedLength) {
            this.pageStart = pageStart;
            this.blocks = blocks;
            this.storedLength = storedLength;
        }
    }

    private static final class SearchInterval {
        private final int lower;
        private final int upper;
        private final byte[] needle;

        private SearchInterval(int lower, int upper, byte[] needle) {
            this.lower = lower;
            this.upper = upper;
            this.needle = needle;
        }

        private boolean isEmpty() {
            return lower >= upper;
        }

        private int size() {
            return upper - lower;
        }
    }

    private static final class RankPair {
        private final int lower;
        private final int upper;

        private RankPair(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private static final class Metadata {
        private final FMIndexFile.Footer footer;
        private final FMIndexFile.Directory directory;

        private Metadata(FMIndexFile.Footer footer, FMIndexFile.Directory directory) {
            this.footer = footer;
            this.directory = directory;
        }
    }

    static final class FileSetRowCountValidator {
        private final long expectedTotalRowCount;
        private final FileRowRange[] ranges;
        private final boolean[] validated;
        private int validatedFiles;
        @Nullable private String failure;

        FileSetRowCountValidator(int fileCount, long expectedTotalRowCount) {
            Preconditions.checkArgument(fileCount > 0, "FM index file count must be positive.");
            Preconditions.checkArgument(
                    expectedTotalRowCount > 0, "FM index total row count must be positive.");
            this.expectedTotalRowCount = expectedTotalRowCount;
            this.ranges = new FileRowRange[fileCount];
            this.validated = new boolean[fileCount];
        }

        synchronized void validate(
                int filePosition, long fileRowCount, long firstRowId, long lastRowId) {
            Preconditions.checkElementIndex(filePosition, ranges.length, "FM index file position");
            if (failure != null) {
                throw new IllegalStateException(failure);
            }
            if (validated[filePosition]) {
                FileRowRange previous = ranges[filePosition];
                Preconditions.checkState(
                        previous.rowCount == fileRowCount
                                && previous.firstRowId == firstRowId
                                && previous.lastRowId == lastRowId,
                        "FM index file row range changed while reading.");
                return;
            }
            if (fileRowCount <= 0
                    || firstRowId < 0
                    || lastRowId < firstRowId
                    || lastRowId >= expectedTotalRowCount) {
                fail(
                        String.format(
                                "FM index file row range [%s, %s] is outside source range [0, %s).",
                                firstRowId, lastRowId, expectedTotalRowCount));
            }
            if (lastRowId - firstRowId != fileRowCount - 1) {
                fail("FM index file row range does not match its row count.");
            }
            validated[filePosition] = true;
            ranges[filePosition] = new FileRowRange(fileRowCount, firstRowId, lastRowId);
            validatedFiles++;
            if (validatedFiles != ranges.length) {
                return;
            }
            long total = 0;
            for (FileRowRange range : ranges) {
                if (total > Long.MAX_VALUE - range.rowCount) {
                    fail("FM index total row count overflow.");
                }
                total += range.rowCount;
            }
            if (total != expectedTotalRowCount) {
                fail(
                        String.format(
                                "FM index row count mismatch: expected=%s, files=%s.",
                                expectedTotalRowCount, total));
            }
            FileRowRange[] ordered = ranges.clone();
            Arrays.sort(ordered, (left, right) -> Long.compare(left.firstRowId, right.firstRowId));
            long expected = 0;
            for (FileRowRange range : ordered) {
                if (range.firstRowId != expected) {
                    fail(
                            String.format(
                                    "FM index file row ranges do not exactly cover source rows: expected row %s, found %s.",
                                    expected, range.firstRowId));
                }
                expected = range.lastRowId + 1;
            }
        }

        private void fail(String message) {
            failure = message;
            throw new IllegalStateException(message);
        }
    }

    private static final class FileRowRange {
        private final long rowCount;
        private final long firstRowId;
        private final long lastRowId;

        private FileRowRange(long rowCount, long firstRowId, long lastRowId) {
            this.rowCount = rowCount;
            this.firstRowId = firstRowId;
            this.lastRowId = lastRowId;
        }
    }
}
