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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.io.CompressedPageFileOutput;
import org.apache.paimon.io.PageFileOutput;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/* This file is based on source code of StorageWriter from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Internal write implementation for hash kv store. */
public class HashLookupStoreWriter implements LookupStoreWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(HashLookupStoreWriter.class.getName());

    // load factor of hash map, default 0.75
    private final double loadFactor;
    // Output
    private final File tempFolder;
    private final File outputFile;
    // Index stream
    private File[] indexFiles;
    private DataOutputStream[] indexStreams;
    // Data stream
    private File[] dataFiles;
    private DataOutputStream[] dataStreams;
    // Cache last value
    private byte[][] lastValues;
    private int[] lastValuesLength;
    // Data length
    private long[] dataLengths;
    // Max offset length
    private int[] maxOffsetLengths;
    // Number of keys
    private int keyCount;
    private int[] keyCounts;
    // Number of values
    private int valueCount;
    // Number of collisions
    private int collisions;

    @Nullable private final BloomFilter.Builder bloomFilter;

    @Nullable private final BlockCompressionFactory compressionFactory;
    private final int compressPageSize;

    HashLookupStoreWriter(
            double loadFactor,
            File file,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory,
            int compressPageSize)
            throws IOException {
        this.loadFactor = loadFactor;
        this.outputFile = file;
        this.compressionFactory = compressionFactory;
        this.compressPageSize = compressPageSize;
        if (loadFactor <= 0.0 || loadFactor >= 1.0) {
            throw new IllegalArgumentException(
                    "Illegal load factor = " + loadFactor + ", should be between 0.0 and 1.0.");
        }

        this.tempFolder = new File(file.getParentFile(), UUID.randomUUID().toString());
        if (!tempFolder.mkdir()) {
            throw new IOException("Can not create temp folder: " + tempFolder);
        }
        this.indexStreams = new DataOutputStream[0];
        this.dataStreams = new DataOutputStream[0];
        this.indexFiles = new File[0];
        this.dataFiles = new File[0];
        this.lastValues = new byte[0][];
        this.lastValuesLength = new int[0];
        this.dataLengths = new long[0];
        this.maxOffsetLengths = new int[0];
        this.keyCounts = new int[0];
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        int keyLength = key.length;

        // Get the Output stream for that keyLength, each key length has its own file
        DataOutputStream indexStream = getIndexStream(keyLength);

        // Write key
        indexStream.write(key);

        // Check if the value is identical to the last inserted
        byte[] lastValue = lastValues[keyLength];
        boolean sameValue = lastValue != null && Arrays.equals(value, lastValue);

        // Get data stream and length
        long dataLength = dataLengths[keyLength];
        if (sameValue) {
            dataLength -= lastValuesLength[keyLength];
        }

        // Write offset and record max offset length
        int offsetLength = VarLengthIntUtils.encodeLong(indexStream, dataLength);
        maxOffsetLengths[keyLength] = Math.max(offsetLength, maxOffsetLengths[keyLength]);

        // Write if data is not the same
        if (!sameValue) {
            // Get stream
            DataOutputStream dataStream = getDataStream(keyLength);

            // Write size and value
            int valueSize = VarLengthIntUtils.encodeInt(dataStream, value.length);
            dataStream.write(value);

            // Update data length
            dataLengths[keyLength] += valueSize + value.length;

            // Update last value
            lastValues[keyLength] = value;
            lastValuesLength[keyLength] = valueSize + value.length;

            valueCount++;
        }

        keyCount++;
        keyCounts[keyLength]++;
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }
    }

    @Override
    public Context close() throws IOException {
        // Close the data and index streams
        for (DataOutputStream dos : dataStreams) {
            if (dos != null) {
                dos.close();
            }
        }
        for (DataOutputStream dos : indexStreams) {
            if (dos != null) {
                dos.close();
            }
        }

        // Stats
        LOG.info("Number of keys: {}", keyCount);
        LOG.info("Number of values: {}", valueCount);

        // Prepare files to merge
        List<File> filesToMerge = new ArrayList<>();

        int bloomFilterBytes = bloomFilter == null ? 0 : bloomFilter.getBuffer().size();
        HashContext context =
                new HashContext(
                        bloomFilter != null,
                        bloomFilter == null ? 0 : bloomFilter.expectedEntries(),
                        bloomFilterBytes,
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new int[keyCounts.length],
                        new long[keyCounts.length],
                        0,
                        null);

        long indexesLength = bloomFilterBytes;
        long datasLength = 0;
        for (int i = 0; i < this.keyCounts.length; i++) {
            if (this.keyCounts[i] > 0) {
                // Write the key Count
                context.keyCounts[i] = keyCounts[i];

                // Write slot count
                int slots = (int) Math.round(keyCounts[i] / loadFactor);
                context.slots[i] = slots;

                // Write slot size
                int offsetLength = maxOffsetLengths[i];
                context.slotSizes[i] = i + offsetLength;

                // Write index offset
                context.indexOffsets[i] = (int) indexesLength;

                // Increment index length
                indexesLength += (long) (i + offsetLength) * slots;

                // Write data length
                context.dataOffsets[i] = datasLength;

                // Increment data length
                datasLength += dataLengths[i];
            }
        }

        // adjust data offsets
        for (int i = 0; i < context.dataOffsets.length; i++) {
            context.dataOffsets[i] = indexesLength + context.dataOffsets[i];
        }

        PageFileOutput output =
                PageFileOutput.create(outputFile, compressPageSize, compressionFactory);
        try {
            // Write bloom filter file
            if (bloomFilter != null) {
                File bloomFilterFile = new File(tempFolder, "bloomfilter.dat");
                bloomFilterFile.deleteOnExit();
                try (FileOutputStream bfOutputStream = new FileOutputStream(bloomFilterFile)) {
                    bfOutputStream.write(bloomFilter.getBuffer().getArray());
                    LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
                }
                filesToMerge.add(bloomFilterFile);
            }

            // Build index file
            for (int i = 0; i < indexFiles.length; i++) {
                if (indexFiles[i] != null) {
                    filesToMerge.add(buildIndex(i));
                }
            }

            // Stats collisions
            LOG.info("Number of collisions: {}", collisions);

            // Add data files
            for (File dataFile : dataFiles) {
                if (dataFile != null) {
                    filesToMerge.add(dataFile);
                }
            }

            // Merge and write to output
            checkFreeDiskSpace(filesToMerge);
            mergeFiles(filesToMerge, output);
        } finally {
            cleanup(filesToMerge);
            output.close();
        }

        LOG.info(
                "Compressed Total store size: {} Mb",
                new DecimalFormat("#,##0.0").format(outputFile.length() / (1024 * 1024)));

        if (output instanceof CompressedPageFileOutput) {
            CompressedPageFileOutput compressedOutput = (CompressedPageFileOutput) output;
            context = context.copy(compressedOutput.uncompressBytes(), compressedOutput.pages());
        }
        return context;
    }

    private File buildIndex(int keyLength) throws IOException {
        long count = keyCounts[keyLength];
        int slots = (int) Math.round(count / loadFactor);
        int offsetLength = maxOffsetLengths[keyLength];
        int slotSize = keyLength + offsetLength;

        // Init index
        File indexFile = new File(tempFolder, "index" + keyLength + ".dat");
        try (RandomAccessFile indexAccessFile = new RandomAccessFile(indexFile, "rw")) {
            indexAccessFile.setLength((long) slots * slotSize);
            FileChannel indexChannel = indexAccessFile.getChannel();
            MappedByteBuffer byteBuffer =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexAccessFile.length());

            // Init reading stream
            File tempIndexFile = indexFiles[keyLength];
            DataInputStream tempIndexStream =
                    new DataInputStream(
                            new BufferedInputStream(new FileInputStream(tempIndexFile)));
            try {
                byte[] keyBuffer = new byte[keyLength];
                byte[] slotBuffer = new byte[slotSize];
                byte[] offsetBuffer = new byte[offsetLength];

                // Read all keys
                for (int i = 0; i < count; i++) {
                    // Read key
                    tempIndexStream.readFully(keyBuffer);

                    // Read offset
                    long offset = VarLengthIntUtils.decodeLong(tempIndexStream);

                    // Hash
                    long hash = MurmurHashUtils.hashBytesPositive(keyBuffer);

                    boolean collision = false;
                    for (int probe = 0; probe < count; probe++) {
                        int slot = (int) ((hash + probe) % slots);
                        byteBuffer.position(slot * slotSize);
                        byteBuffer.get(slotBuffer);

                        long found = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);
                        if (found == 0) {
                            // The spot is empty use it
                            byteBuffer.position(slot * slotSize);
                            byteBuffer.put(keyBuffer);
                            int pos = VarLengthIntUtils.encodeLong(offsetBuffer, offset);
                            byteBuffer.put(offsetBuffer, 0, pos);
                            break;
                        } else {
                            collision = true;
                            // Check for duplicates
                            if (Arrays.equals(keyBuffer, Arrays.copyOf(slotBuffer, keyLength))) {
                                throw new RuntimeException(
                                        String.format(
                                                "A duplicate key has been found for for key bytes %s",
                                                Arrays.toString(keyBuffer)));
                            }
                        }
                    }

                    if (collision) {
                        collisions++;
                    }
                }

                String msg =
                        "  Max offset length: "
                                + offsetLength
                                + " bytes"
                                + "\n  Slot size: "
                                + slotSize
                                + " bytes";

                LOG.info("Built index file {}\n" + msg, indexFile.getName());
            } finally {
                // Close input
                tempIndexStream.close();

                // Close index and make sure resources are liberated
                indexChannel.close();

                // Delete temp index file
                if (tempIndexFile.delete()) {
                    LOG.info("Temporary index file {} has been deleted", tempIndexFile.getName());
                }
            }
        }

        return indexFile;
    }

    // Fail if the size of the expected store file exceed 2/3rd of the free disk space
    private void checkFreeDiskSpace(List<File> inputFiles) {
        // Check for free space
        long usableSpace = 0;
        long totalSize = 0;
        for (File f : inputFiles) {
            if (f.exists()) {
                totalSize += f.length();
                usableSpace = f.getUsableSpace();
            }
        }
        LOG.info(
                "Total expected store size is {} Mb",
                new DecimalFormat("#,##0.0").format(totalSize / (1024 * 1024)));
        LOG.info(
                "Usable free space on the system is {} Mb",
                new DecimalFormat("#,##0.0").format(usableSpace / (1024 * 1024)));
        if (totalSize / (double) usableSpace >= 0.66) {
            throw new RuntimeException("Aborting because there isn' enough free disk space");
        }
    }

    // Merge files to the provided fileChannel
    private void mergeFiles(List<File> inputFiles, PageFileOutput output) throws IOException {
        long startTime = System.nanoTime();

        // Merge files
        for (File f : inputFiles) {
            if (f.exists()) {
                FileInputStream fileInputStream = new FileInputStream(f);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                try {
                    LOG.info("Merging {} size={}", f.getName(), f.length());

                    byte[] buffer = new byte[8192];
                    int length;
                    while ((length = bufferedInputStream.read(buffer)) > 0) {
                        output.write(buffer, 0, length);
                    }
                } finally {
                    bufferedInputStream.close();
                    fileInputStream.close();
                }
            } else {
                LOG.info("Skip merging file {} because it doesn't exist", f.getName());
            }
        }

        LOG.info("Time to merge {} s", ((System.nanoTime() - startTime) / 1000000000.0));
    }

    // Cleanup files
    private void cleanup(List<File> inputFiles) {
        for (File f : inputFiles) {
            if (f.exists()) {
                if (f.delete()) {
                    LOG.info("Deleted temporary file {}", f.getName());
                }
            }
        }
        if (tempFolder.delete()) {
            LOG.info("Deleted temporary folder at {}", tempFolder.getAbsolutePath());
        }
    }

    // Get the data stream for the specified keyLength, create it if needed
    private DataOutputStream getDataStream(int keyLength) throws IOException {
        // Resize array if necessary
        if (dataStreams.length <= keyLength) {
            dataStreams = Arrays.copyOf(dataStreams, keyLength + 1);
            dataFiles = Arrays.copyOf(dataFiles, keyLength + 1);
        }

        DataOutputStream dos = dataStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "data" + keyLength + ".dat");
            file.deleteOnExit();
            dataFiles[keyLength] = file;

            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            dataStreams[keyLength] = dos;

            // Write one byte so the zero offset is reserved
            dos.writeByte(0);
        }
        return dos;
    }

    // Get the index stream for the specified keyLength, create it if needed
    private DataOutputStream getIndexStream(int keyLength) throws IOException {
        // Resize array if necessary
        if (indexStreams.length <= keyLength) {
            indexStreams = Arrays.copyOf(indexStreams, keyLength + 1);
            indexFiles = Arrays.copyOf(indexFiles, keyLength + 1);
            keyCounts = Arrays.copyOf(keyCounts, keyLength + 1);
            maxOffsetLengths = Arrays.copyOf(maxOffsetLengths, keyLength + 1);
            lastValues = Arrays.copyOf(lastValues, keyLength + 1);
            lastValuesLength = Arrays.copyOf(lastValuesLength, keyLength + 1);
            dataLengths = Arrays.copyOf(dataLengths, keyLength + 1);
        }

        // Get or create stream
        DataOutputStream dos = indexStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "temp_index" + keyLength + ".dat");
            file.deleteOnExit();
            indexFiles[keyLength] = file;

            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            indexStreams[keyLength] = dos;

            dataLengths[keyLength]++;
        }
        return dos;
    }
}
