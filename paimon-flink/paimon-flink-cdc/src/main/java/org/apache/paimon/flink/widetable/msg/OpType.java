package org.apache.paimon.flink.widetable.msg;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum OpType {
    INSERT("I", "INSERT"),
    UPDATE("U", "UPDATE"),
    DELETE("D", "DELETE");

    private String[] code;

    private static final Map<String, OpType> opTypes =
            Stream.of(OpType.values())
                    .flatMap(
                            item ->
                                    Stream.of(item.getCode())
                                            .map(
                                                    code ->
                                                            Entry.<String, OpType>builder()
                                                                    .key(code)
                                                                    .value(item)
                                                                    .build()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    OpType(String... code) {
        this.code = code;
    }

    public static OpType getOpType(String str) {
        return opTypes.get(str);
    }

    public static void main(String[] args) {
        System.out.println(opTypes);
    }
}
