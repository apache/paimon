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

package org.apache.paimon.format.sst.layout;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** An SST File Reader to serve scan queries. */
public class SstFileScanReader extends AbstractSstFileReader {
    private BlockIterator currentDataIterator;

    public SstFileScanReader(
            SeekableInputStream input,
            Comparator<MemorySlice> comparator,
            long fileSize,
            Path filePath,
            @Nullable BlockCache blockCache)
            throws IOException {
        super(input, comparator, fileSize, filePath, blockCache);
        this.currentDataIterator = null;
    }

    /**
     * Seek to the position of the record whose key is equal to the key or is the smallest element
     * greater than the given key.
     *
     * @param key the key to seek
     * @return record position of the seeked record, -1 if not found.
     */
    public int seekTo(byte[] key) throws IOException {
        if (footer.getRowCount() == 0) {
            return -1;
        }
        MemorySlice keySlice = MemorySlice.wrap(key);
        // find candidate index block
        indexBlockIterator.seekTo(keySlice);
        if (indexBlockIterator.hasNext()) {
            // avoid fetching data block if the key is smaller than firstKey
            if (comparator.compare(firstKey, keySlice) > 0) {
                return 0;
            }
            IndexEntryHandle entryHandle =
                    IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
            currentDataIterator =
                    readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()))
                            .iterator();
            currentDataIterator.seekTo(keySlice);
            int recordCount = currentDataIterator.getRecordCount();
            int positionInBlock = currentDataIterator.getRecordPosition();
            Preconditions.checkState(
                    positionInBlock >= 0, "Position inside data block should >= 0, it's a bug.");
            return entryHandle.getLastRecordPosition() - (recordCount - positionInBlock - 1);
        }
        return -1;
    }

    /**
     * Seek to the specified record position.
     *
     * @param position position
     */
    public void seekTo(int position) throws IOException {
        if (position < 0 || position >= footer.getRowCount()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of range, file %s only have %s rows, but trying to seek to %s",
                            filePath, footer.getRowCount(), position));
        }

        // 1. seekTo the block entry exactly containing the position
        indexBlockIterator.seekTo(
                position,
                Comparator.naturalOrder(),
                entry -> IndexEntryHandle.read(entry.getValue().toInput()).getLastRecordPosition());

        // 2. fetch the data block
        Preconditions.checkState(indexBlockIterator.hasNext());
        IndexEntryHandle entryHandle =
                IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
        currentDataIterator =
                readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()))
                        .iterator();

        // 3. seek to the inner position of data block
        int positionInBlock =
                position
                        - (entryHandle.getLastRecordPosition()
                                - currentDataIterator.getRecordCount()
                                + 1);
        currentDataIterator.seekTo(positionInBlock);
    }

    /**
     * Read a batch of records from current position.
     *
     * @return a batch of entries, null if reaching file end
     */
    public BlockIterator readBatch() throws IOException {
        BlockIterator result = null;
        if (currentDataIterator == null || !currentDataIterator.hasNext()) {
            // reach file end
            if (!indexBlockIterator.hasNext()) {
                return null;
            }
            currentDataIterator = getNextBlock(indexBlockIterator);
        }
        result = currentDataIterator;
        currentDataIterator = null;
        return result;
    }
}
