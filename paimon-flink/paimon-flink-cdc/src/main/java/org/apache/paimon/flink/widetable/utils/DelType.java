package org.apache.paimon.flink.widetable.utils;

public enum DelType {
    DEL(1),
    NOTDEL(0);

    private Integer value;

    DelType(Integer value) {
        this.value = value;
    }
}
