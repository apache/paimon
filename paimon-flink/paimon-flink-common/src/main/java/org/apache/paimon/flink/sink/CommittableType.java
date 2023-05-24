package org.apache.paimon.flink.sink;

public interface CommittableType {
    long checkpointId();

    Kind kind();

    Object wrappedCommittable();

    enum Kind {
        FILE((byte) 0),

        LOG_OFFSET((byte) 1);

        private final byte value;

        Kind(byte value) {
            this.value = value;
        }

        public byte toByteValue() {
            return value;
        }

        public static Kind fromByteValue(byte value) {
            switch (value) {
                case 0:
                    return FILE;
                case 1:
                    return LOG_OFFSET;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported byte value '" + value + "' for value kind.");
            }
        }
    }
}
