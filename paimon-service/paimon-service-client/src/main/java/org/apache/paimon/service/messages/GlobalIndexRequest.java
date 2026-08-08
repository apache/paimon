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

package org.apache.paimon.service.messages;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageDeserializer;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNSUPPORTED_PROTOCOL;
import static org.apache.paimon.service.network.messages.MessageDeserializer.readBytes;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Request for keys which all belong to the same logical global-index shard. */
public class GlobalIndexRequest extends MessageBody {

    public static final int PROTOCOL_VERSION = 2;
    public static final int MAX_KEYS = 10_000;
    public static final int MAX_TOTAL_KEY_BYTES = 16 * 1024 * 1024;
    public static final int MAX_SERVER_EPOCH_BYTES = 256;

    /** Maximum request payload before the network envelope is added. */
    public static final int MAX_SERIALIZED_PAYLOAD_BYTES =
            Integer.BYTES
                    + Integer.BYTES
                    + MAX_SERVER_EPOCH_BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + MAX_KEYS * Integer.BYTES
                    + MAX_TOTAL_KEY_BYTES;

    /** Maximum complete network frame, including length, message header, and request ID fields. */
    public static final int MAX_NETWORK_FRAME_BYTES =
            Integer.BYTES + 2 * Integer.BYTES + Long.BYTES + MAX_SERIALIZED_PAYLOAD_BYTES;

    private final String serverEpoch;
    private final long servedGeneration;
    private final BinaryRow[] keys;

    public GlobalIndexRequest(String serverEpoch, long servedGeneration, BinaryRow[] keys) {
        validateServerEpoch(serverEpoch);
        validateKeyCount(keys == null ? 0 : keys.length);
        long totalKeyBytes = 0L;
        BinaryRow[] normalizedKeys = new BinaryRow[keys.length];
        for (int i = 0; i < keys.length; i++) {
            BinaryRow key = keys[i];
            if (key == null) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST, "Global-index request key is null.");
            }
            BinaryRow normalizedKey = GlobalIndexQueryServiceUtils.normalizeKey(key);
            validateKey(normalizedKey);
            totalKeyBytes += Integer.BYTES + normalizedKey.getSizeInBytes();
            validateTotalKeyBytes(totalKeyBytes);
            normalizedKeys[i] = normalizedKey;
        }
        this.serverEpoch = serverEpoch;
        this.servedGeneration = servedGeneration;
        this.keys = normalizedKeys;
    }

    public int protocolVersion() {
        return PROTOCOL_VERSION;
    }

    public String serverEpoch() {
        return serverEpoch;
    }

    public long servedGeneration() {
        return servedGeneration;
    }

    public BinaryRow[] keys() {
        return keys;
    }

    @Override
    public byte[] serialize() {
        byte[] serverEpochBytes = serverEpoch.getBytes(StandardCharsets.UTF_8);
        int size =
                Integer.BYTES
                        + Integer.BYTES
                        + serverEpochBytes.length
                        + Long.BYTES
                        + Integer.BYTES;
        List<byte[]> serializedKeys = new ArrayList<>(keys.length);
        for (BinaryRow key : keys) {
            byte[] bytes = serializeBinaryRow(key);
            serializedKeys.add(bytes);
            size += Integer.BYTES + bytes.length;
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(size)
                        .putInt(PROTOCOL_VERSION)
                        .putInt(serverEpochBytes.length)
                        .put(serverEpochBytes)
                        .putLong(servedGeneration)
                        .putInt(keys.length);
        for (byte[] key : serializedKeys) {
            buffer.putInt(key.length).put(key);
        }
        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || (o instanceof GlobalIndexRequest
                        && serverEpoch.equals(((GlobalIndexRequest) o).serverEpoch)
                        && servedGeneration == ((GlobalIndexRequest) o).servedGeneration
                        && Arrays.equals(keys, ((GlobalIndexRequest) o).keys));
    }

    @Override
    public int hashCode() {
        int result = serverEpoch.hashCode();
        result = 31 * result + Long.hashCode(servedGeneration);
        return 31 * result + Arrays.hashCode(keys);
    }

    /** Deserializer for {@link GlobalIndexRequest}. */
    public static class Deserializer implements MessageDeserializer<GlobalIndexRequest> {

        @Override
        public GlobalIndexRequest deserializeMessage(ByteBuf buf) {
            try {
                return deserialize(buf);
            } catch (GlobalIndexQueryException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST, "Malformed global-index query request.", e);
            }
        }

        private GlobalIndexRequest deserialize(ByteBuf buf) {
            int protocolVersion = buf.readInt();
            if (protocolVersion != PROTOCOL_VERSION) {
                throw new GlobalIndexQueryException(
                        UNSUPPORTED_PROTOCOL,
                        String.format(
                                "Unsupported global-index query protocol version %s; expected %s.",
                                protocolVersion, PROTOCOL_VERSION));
            }
            int serverEpochLength = buf.readInt();
            if (serverEpochLength <= 0) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        "Global-index request contains an invalid server epoch length.");
            }
            if (serverEpochLength > MAX_SERVER_EPOCH_BYTES) {
                throw new GlobalIndexQueryException(
                        REQUEST_TOO_LARGE,
                        String.format(
                                "Global-index request server epoch is %s bytes; maximum is %s.",
                                serverEpochLength, MAX_SERVER_EPOCH_BYTES));
            }
            String serverEpoch =
                    new String(readBytes(buf, serverEpochLength), StandardCharsets.UTF_8);
            validateServerEpoch(serverEpoch);
            long servedGeneration = buf.readLong();
            int size = buf.readInt();
            validateKeyCount(size);
            List<BinaryRow> keys = new ArrayList<>(size);
            long totalKeyBytes = 0L;
            for (int i = 0; i < size; i++) {
                int keyLength = buf.readInt();
                if (keyLength <= 0) {
                    throw new GlobalIndexQueryException(
                            INVALID_REQUEST,
                            "Global-index request contains an invalid serialized key length.");
                }
                totalKeyBytes += keyLength;
                validateTotalKeyBytes(totalKeyBytes);
                byte[] serializedKey = readBytes(buf, keyLength);
                validateSerializedKey(serializedKey);
                // The constructor copies this non-zero-offset row and canonicalizes its RowKind.
                keys.add(deserializeBinaryRow(serializedKey));
            }
            if (buf.isReadable()) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        String.format(
                                "Global-index request contains %s trailing bytes.",
                                buf.readableBytes()));
            }
            return new GlobalIndexRequest(
                    serverEpoch, servedGeneration, keys.toArray(new BinaryRow[0]));
        }
    }

    private static void validateServerEpoch(String serverEpoch) {
        if (serverEpoch == null || serverEpoch.isEmpty()) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index request server epoch is empty.");
        }
        int bytes = serverEpoch.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_SERVER_EPOCH_BYTES) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index request server epoch is %s bytes; maximum is %s.",
                            bytes, MAX_SERVER_EPOCH_BYTES));
        }
    }

    private static void validateKeyCount(int keyCount) {
        if (keyCount <= 0) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index request keys are empty.");
        }
        if (keyCount > MAX_KEYS) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index request contains %s keys; maximum is %s.",
                            keyCount, MAX_KEYS));
        }
    }

    private static void validateKey(BinaryRow key) {
        if (key.getFieldCount() != 1) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST,
                    String.format(
                            "Global-index request key must contain exactly one field, but found %s.",
                            key.getFieldCount()));
        }
        if (key.isNullAt(0)) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index request key field is null.");
        }
    }

    private static void validateSerializedKey(byte[] serializedKey) {
        if (serializedKey.length < Integer.BYTES) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index request contains a truncated key.");
        }
        int arity = ByteBuffer.wrap(serializedKey).getInt();
        if (arity != 1) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST,
                    String.format(
                            "Global-index request key must contain exactly one field, but found %s.",
                            arity));
        }
        int rowBytes = serializedKey.length - Integer.BYTES;
        if (rowBytes < BinaryRow.calculateFixPartSizeInBytes(arity)) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index request contains a truncated key row.");
        }
    }

    private static void validateTotalKeyBytes(long totalKeyBytes) {
        if (totalKeyBytes > MAX_TOTAL_KEY_BYTES) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index request key payload is %s bytes; maximum is %s.",
                            totalKeyBytes, MAX_TOTAL_KEY_BYTES));
        }
    }
}
