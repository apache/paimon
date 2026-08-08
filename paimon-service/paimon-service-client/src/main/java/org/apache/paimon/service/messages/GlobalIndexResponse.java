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
import org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageDeserializer;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNSUPPORTED_PROTOCOL;
import static org.apache.paimon.service.network.messages.MessageDeserializer.readBytes;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Snapshot-fenced response from one global-index query server. */
public class GlobalIndexResponse extends MessageBody {

    public static final int MAX_TOTAL_VALUE_BYTES =
            GlobalIndexQueryServiceUtils.MAX_TOTAL_VALUE_BYTES;
    public static final int MAX_ERROR_MESSAGE_BYTES = 16 * 1024;
    private static final int MAX_VALUE_FIELDS = 10_000;
    private static final int SUCCESS_CODE = 0;

    /** Maximum response payload before the network envelope is added. */
    public static final int MAX_SERIALIZED_PAYLOAD_BYTES =
            Integer.BYTES
                    + Integer.BYTES
                    + GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + 1
                    + Integer.BYTES
                    + MAX_ERROR_MESSAGE_BYTES
                    + Integer.BYTES
                    + GlobalIndexRequest.MAX_KEYS
                    + GlobalIndexRequest.MAX_KEYS * Integer.BYTES
                    + MAX_TOTAL_VALUE_BYTES;

    /** Maximum complete network frame, including length, message header, and request ID fields. */
    public static final int MAX_NETWORK_FRAME_BYTES =
            Integer.BYTES + 2 * Integer.BYTES + Long.BYTES + MAX_SERIALIZED_PAYLOAD_BYTES;

    private final String serverEpoch;
    private final long servedGeneration;
    private final long servedSnapshotId;
    @Nullable private final GlobalIndexQueryErrorCode errorCode;
    private final boolean retryable;
    private final String errorMessage;
    private final BinaryRow[] values;

    public GlobalIndexResponse(
            String serverEpoch, long servedGeneration, long servedSnapshotId, BinaryRow[] values) {
        this(serverEpoch, servedGeneration, servedSnapshotId, null, false, "", values);
    }

    private GlobalIndexResponse(
            String serverEpoch,
            long servedGeneration,
            long servedSnapshotId,
            @Nullable GlobalIndexQueryErrorCode errorCode,
            boolean retryable,
            String errorMessage,
            BinaryRow[] values) {
        validateServerEpoch(serverEpoch);
        validateStatus(errorCode, retryable, errorMessage);
        validateValueCount(values == null ? -1 : values.length);
        if (errorCode != null && values.length != 0) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Failed global-index response must not contain values.");
        }
        long totalValueBytes = 0L;
        for (BinaryRow value : values) {
            if (value != null) {
                validateValue(value);
                totalValueBytes += Integer.BYTES + value.getSizeInBytes();
                validateTotalValueBytes(totalValueBytes);
            }
        }
        this.serverEpoch = serverEpoch;
        this.servedGeneration = servedGeneration;
        this.servedSnapshotId = servedSnapshotId;
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.errorMessage = errorMessage;
        this.values = values;
    }

    public static GlobalIndexResponse failure(
            String serverEpoch,
            long servedGeneration,
            long servedSnapshotId,
            GlobalIndexQueryErrorCode errorCode,
            String message) {
        return failure(
                serverEpoch,
                servedGeneration,
                servedSnapshotId,
                errorCode,
                errorCode.retryable(),
                message);
    }

    public static GlobalIndexResponse failure(
            String serverEpoch,
            long servedGeneration,
            long servedSnapshotId,
            GlobalIndexQueryErrorCode errorCode,
            boolean retryable,
            String message) {
        return new GlobalIndexResponse(
                serverEpoch,
                servedGeneration,
                servedSnapshotId,
                errorCode,
                retryable,
                truncateErrorMessage(message),
                new BinaryRow[0]);
    }

    public int protocolVersion() {
        return GlobalIndexRequest.PROTOCOL_VERSION;
    }

    public String serverEpoch() {
        return serverEpoch;
    }

    public long servedGeneration() {
        return servedGeneration;
    }

    public long servedSnapshotId() {
        return servedSnapshotId;
    }

    public boolean isSuccess() {
        return errorCode == null;
    }

    @Nullable
    public GlobalIndexQueryErrorCode errorCode() {
        return errorCode;
    }

    public boolean retryable() {
        return retryable;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public GlobalIndexQueryException toException() {
        if (errorCode == null) {
            throw new IllegalStateException("Successful global-index response has no error.");
        }
        return new GlobalIndexQueryException(errorCode, retryable, errorMessage);
    }

    public BinaryRow[] values() {
        return values;
    }

    @Override
    public byte[] serialize() {
        byte[] serverEpochBytes = serverEpoch.getBytes(StandardCharsets.UTF_8);
        byte[] errorMessageBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        byte[][] serializedValues = new byte[values.length][];
        int size =
                Integer.BYTES
                        + Integer.BYTES
                        + serverEpochBytes.length
                        + Long.BYTES
                        + Long.BYTES
                        + Integer.BYTES
                        + 1
                        + Integer.BYTES
                        + errorMessageBytes.length
                        + Integer.BYTES;
        for (int i = 0; i < values.length; i++) {
            size += 1;
            if (values[i] != null) {
                serializedValues[i] = serializeBinaryRow(values[i]);
                size += Integer.BYTES + serializedValues[i].length;
            }
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(size)
                        .putInt(GlobalIndexRequest.PROTOCOL_VERSION)
                        .putInt(serverEpochBytes.length)
                        .put(serverEpochBytes)
                        .putLong(servedGeneration)
                        .putLong(servedSnapshotId)
                        .putInt(errorCode == null ? SUCCESS_CODE : errorCode.wireCode())
                        .put((byte) (retryable ? 1 : 0))
                        .putInt(errorMessageBytes.length)
                        .put(errorMessageBytes)
                        .putInt(values.length);
        for (byte[] serializedValue : serializedValues) {
            if (serializedValue == null) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0).putInt(serializedValue.length).put(serializedValue);
            }
        }
        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GlobalIndexResponse)) {
            return false;
        }
        GlobalIndexResponse that = (GlobalIndexResponse) o;
        return servedGeneration == that.servedGeneration
                && servedSnapshotId == that.servedSnapshotId
                && retryable == that.retryable
                && serverEpoch.equals(that.serverEpoch)
                && errorCode == that.errorCode
                && errorMessage.equals(that.errorMessage)
                && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = serverEpoch.hashCode();
        result = 31 * result + Long.hashCode(servedGeneration);
        result = 31 * result + Long.hashCode(servedSnapshotId);
        result = 31 * result + (errorCode == null ? 0 : errorCode.hashCode());
        result = 31 * result + Boolean.hashCode(retryable);
        result = 31 * result + errorMessage.hashCode();
        return 31 * result + Arrays.hashCode(values);
    }

    /** Deserializer for {@link GlobalIndexResponse}. */
    public static class Deserializer implements MessageDeserializer<GlobalIndexResponse> {

        @Override
        public GlobalIndexResponse deserializeMessage(ByteBuf buf) {
            try {
                return deserialize(buf);
            } catch (GlobalIndexQueryException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST, "Malformed global-index query response.", e);
            }
        }

        private GlobalIndexResponse deserialize(ByteBuf buf) {
            int protocolVersion = buf.readInt();
            if (protocolVersion != GlobalIndexRequest.PROTOCOL_VERSION) {
                throw new GlobalIndexQueryException(
                        UNSUPPORTED_PROTOCOL,
                        String.format(
                                "Unsupported global-index query protocol version %s; expected %s.",
                                protocolVersion, GlobalIndexRequest.PROTOCOL_VERSION));
            }
            int serverEpochLength = buf.readInt();
            if (serverEpochLength <= 0) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        "Global-index response contains an invalid server epoch length.");
            }
            if (serverEpochLength > GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES) {
                throw new GlobalIndexQueryException(
                        REQUEST_TOO_LARGE,
                        String.format(
                                "Global-index response server epoch is %s bytes; maximum is %s.",
                                serverEpochLength, GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES));
            }
            String serverEpoch =
                    new String(readBytes(buf, serverEpochLength), StandardCharsets.UTF_8);
            validateServerEpoch(serverEpoch);
            long servedGeneration = buf.readLong();
            long servedSnapshotId = buf.readLong();
            int wireErrorCode = buf.readInt();
            GlobalIndexQueryErrorCode errorCode =
                    wireErrorCode == SUCCESS_CODE
                            ? null
                            : GlobalIndexQueryErrorCode.fromWireCode(wireErrorCode);
            byte retryableMarker = buf.readByte();
            if (retryableMarker != 0 && retryableMarker != 1) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        "Global-index response contains an invalid retryable marker.");
            }
            boolean retryable = retryableMarker == 1;
            int errorMessageLength = buf.readInt();
            if (errorMessageLength < 0) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        "Global-index response contains an invalid error message length.");
            }
            if (errorMessageLength > MAX_ERROR_MESSAGE_BYTES) {
                throw new GlobalIndexQueryException(
                        REQUEST_TOO_LARGE,
                        String.format(
                                "Global-index response error message is %s bytes; maximum is %s.",
                                errorMessageLength, MAX_ERROR_MESSAGE_BYTES));
            }
            String errorMessage =
                    new String(readBytes(buf, errorMessageLength), StandardCharsets.UTF_8);
            validateStatus(errorCode, retryable, errorMessage);
            int valueCount = buf.readInt();
            validateValueCount(valueCount);
            if (errorCode != null && valueCount != 0) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST, "Failed global-index response contains values.");
            }

            BinaryRow[] values = new BinaryRow[valueCount];
            long totalValueBytes = 0L;
            for (int i = 0; i < valueCount; i++) {
                byte nullMarker = buf.readByte();
                if (nullMarker != 0 && nullMarker != 1) {
                    throw new GlobalIndexQueryException(
                            INVALID_REQUEST,
                            "Global-index response contains an invalid null marker.");
                }
                boolean isNull = nullMarker == 1;
                if (!isNull) {
                    int valueLength = buf.readInt();
                    if (valueLength <= 0) {
                        throw new GlobalIndexQueryException(
                                INVALID_REQUEST,
                                "Global-index response contains an invalid value length.");
                    }
                    totalValueBytes += valueLength;
                    validateTotalValueBytes(totalValueBytes);
                    byte[] serializedValue = readBytes(buf, valueLength);
                    validateSerializedValue(serializedValue);
                    // Normalize schemaless rows at the protocol boundary. This gives callers an
                    // independent offset-zero row and avoids offset-insensitive BinaryRow methods
                    // reading the serialized arity prefix as row data.
                    values[i] = deserializeBinaryRow(serializedValue).copy();
                }
            }
            if (buf.isReadable()) {
                throw new GlobalIndexQueryException(
                        INVALID_REQUEST,
                        String.format(
                                "Global-index response contains %s trailing bytes.",
                                buf.readableBytes()));
            }
            return new GlobalIndexResponse(
                    serverEpoch,
                    servedGeneration,
                    servedSnapshotId,
                    errorCode,
                    retryable,
                    errorMessage,
                    values);
        }
    }

    private static void validateStatus(
            @Nullable GlobalIndexQueryErrorCode errorCode, boolean retryable, String errorMessage) {
        if (errorMessage == null) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index response error message is null.");
        }
        int messageBytes = errorMessage.getBytes(StandardCharsets.UTF_8).length;
        if (messageBytes > MAX_ERROR_MESSAGE_BYTES) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index response error message is %s bytes; maximum is %s.",
                            messageBytes, MAX_ERROR_MESSAGE_BYTES));
        }
        if (errorCode == null && (retryable || !errorMessage.isEmpty())) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Successful global-index response contains failure metadata.");
        }
    }

    private static String truncateErrorMessage(String message) {
        String nonNullMessage = message == null ? "" : message;
        byte[] bytes = nonNullMessage.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_ERROR_MESSAGE_BYTES) {
            return nonNullMessage;
        }
        int length = MAX_ERROR_MESSAGE_BYTES;
        while (length > 0 && (bytes[length] & 0xc0) == 0x80) {
            length--;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    private static void validateServerEpoch(String serverEpoch) {
        if (serverEpoch == null || serverEpoch.isEmpty()) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index response server epoch is empty.");
        }
        int bytes = serverEpoch.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index response server epoch is %s bytes; maximum is %s.",
                            bytes, GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES));
        }
    }

    private static void validateValueCount(int valueCount) {
        if (valueCount < 0) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index response values are null.");
        }
        if (valueCount > GlobalIndexRequest.MAX_KEYS) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index response contains %s values; maximum is %s.",
                            valueCount, GlobalIndexRequest.MAX_KEYS));
        }
    }

    private static void validateValue(BinaryRow value) {
        if (value.getFieldCount() <= 0 || value.getFieldCount() > MAX_VALUE_FIELDS) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST,
                    String.format(
                            "Global-index response value contains an invalid field count %s.",
                            value.getFieldCount()));
        }
    }

    private static void validateSerializedValue(byte[] serializedValue) {
        if (serializedValue.length < Integer.BYTES) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index response contains a truncated value.");
        }
        int arity = ByteBuffer.wrap(serializedValue).getInt();
        if (arity <= 0 || arity > MAX_VALUE_FIELDS) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST,
                    String.format(
                            "Global-index response value contains an invalid field count %s.",
                            arity));
        }
        int rowBytes = serializedValue.length - Integer.BYTES;
        if (rowBytes < BinaryRow.calculateFixPartSizeInBytes(arity)) {
            throw new GlobalIndexQueryException(
                    INVALID_REQUEST, "Global-index response contains a truncated value row.");
        }
    }

    public static void validateTotalValueBytes(long totalValueBytes) {
        if (totalValueBytes > MAX_TOTAL_VALUE_BYTES) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index response value payload is %s bytes; maximum is %s.",
                            totalValueBytes, MAX_TOTAL_VALUE_BYTES));
        }
    }
}
