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

package org.apache.paimon.format.avro;

import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.RawBlock;
import org.apache.avro.file.RawBlockReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/**
 * Reader which exposes raw blocks from an Avro object container file.
 *
 * <p>This reader owns the input stream and closes it when construction fails or {@link #close()} is
 * called.
 */
public final class AvroBlockReader implements Closeable {

    private final RawBlockReader reader;

    private @Nullable AvroRawBlock borrowedRawBlock;

    public AvroBlockReader(InputStream input) throws IOException {
        try {
            this.reader = new RawBlockReader(input);
        } catch (IOException | RuntimeException | Error e) {
            IOUtils.closeQuietly(input);
            throw e;
        }
    }

    /** Creates a record decoder from the writer schema stored in the Avro file header. */
    public AvroRecordDecoder createRecordDecoder() {
        return new AvroRecordDecoder(reader.getSchema());
    }

    /** Returns whether blocks use the default Avro schema for the given row type. */
    public boolean supportsRawBlockCopy(RowType rowType) {
        return hasSameBinaryLayout(
                AvroSchemaConverter.convertToSchema(rowType, Collections.emptyMap()),
                reader.getSchema());
    }

    static boolean hasSameBinaryLayout(Schema expected, Schema actual) {
        if (expected.getType() != actual.getType()) {
            return false;
        }
        switch (expected.getType()) {
            case RECORD:
                if (expected.getFields().size() != actual.getFields().size()) {
                    return false;
                }
                for (int i = 0; i < expected.getFields().size(); i++) {
                    if (!expected.getFields().get(i).name().equals(actual.getFields().get(i).name())
                            || !hasSameBinaryLayout(
                                    expected.getFields().get(i).schema(),
                                    actual.getFields().get(i).schema())) {
                        return false;
                    }
                }
                return true;
            case ARRAY:
                return hasSameBinaryLayout(expected.getElementType(), actual.getElementType());
            case MAP:
                return hasSameBinaryLayout(expected.getValueType(), actual.getValueType());
            case UNION:
                if (expected.getTypes().size() != actual.getTypes().size()) {
                    return false;
                }
                for (int i = 0; i < expected.getTypes().size(); i++) {
                    if (!hasSameBinaryLayout(
                            expected.getTypes().get(i), actual.getTypes().get(i))) {
                        return false;
                    }
                }
                return true;
            case FIXED:
                return expected.getFixedSize() == actual.getFixedSize();
            case ENUM:
                return expected.getEnumSymbols().equals(actual.getEnumSymbols());
            default:
                return true;
        }
    }

    /** Returns whether another block is available. */
    public boolean hasNextBlock() throws IOException {
        return replaceAvroRuntimeException(reader::hasNextRawBlock);
    }

    /**
     * Returns a borrowed view of the next compressed block.
     *
     * <p>The returned holder and its storage are owned by this reader and reused by the next call
     * to this method. Consume the block before advancing this reader.
     */
    public AvroRawBlock nextBorrowedRawBlock() throws IOException {
        RawBlock block =
                replaceAvroRuntimeException(
                        () ->
                                reader.nextRawBlock(
                                        borrowedRawBlock == null
                                                ? null
                                                : borrowedRawBlock.rawBlock()));
        borrowedRawBlock =
                borrowedRawBlock == null
                        ? new AvroRawBlock(block)
                        : borrowedRawBlock.replace(block);
        return borrowedRawBlock;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static <T> T replaceAvroRuntimeException(IOSupplier<T> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (AvroRuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @FunctionalInterface
    private interface IOSupplier<T> {

        T get() throws IOException;
    }
}
