package org.apache.paimon.flink.action.widetable.utils;

public enum DelType {
    DEL(1),
    NOTDEL(0);

    private Integer value;

    DelType(Integer value) {
        this.value = value;
    }
}
