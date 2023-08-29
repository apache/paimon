package org.apache.paimon.flink.action;

import javax.annotation.Nullable;

import java.io.Serializable;

/** this is a doc. */
public enum DatabaseCompactMode implements Serializable {
    DIVIDED,
    COMBINED;

    public static DatabaseCompactMode fromString(@Nullable String mode) {
        if (mode == null) {
            return DIVIDED;
        }

        switch (mode.toLowerCase()) {
            case "divided":
                return DIVIDED;
            case "combined":
                return COMBINED;
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }
    }
}
