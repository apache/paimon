package org.apache.paimon.flink.widetable.msg;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

public enum CollectType {
    CDC_MYSQL(
            "mysql",
            value -> {
                MysqlCDCMessage canalMessage = JSONObject.parseObject(value, MysqlCDCMessage.class);
                OpType opType = OpType.getOpType(canalMessage.getType());

                Map<String, String> data = null;
                LinkedHashMap<String, String> changeColunms = new LinkedHashMap<>();
                switch (opType) {
                    case INSERT:
                        data = canalMessage.getAfter();
                        break;
                    case UPDATE:
                        LinkedHashMap<String, String> bfColunms = canalMessage.getBefore();
                        LinkedHashMap<String, String> afColunms = canalMessage.getAfter();
                        for (Map.Entry<String, String> entry : afColunms.entrySet()) {
                            String key = entry.getKey();
                            String afvalue = entry.getValue();
                            if (bfColunms.containsKey(key)) {
                                String bfvalue = bfColunms.get(key);
                                if (bfvalue == null && afvalue == null) {
                                    continue;
                                } else if (bfvalue != null && bfvalue.equals(afvalue)) {
                                    continue;
                                }
                            }
                            changeColunms.put(key, afvalue);
                        }

                        bfColunms.putAll(afColunms);
                        data = bfColunms;
                        break;
                    case DELETE:
                        data = canalMessage.getBefore();
                        break;
                }

                if (changeColunms.size() == 0) { // INSERT or DELETE
                    for (Map.Entry<String, String> entry : data.entrySet()) {
                        String key = entry.getKey();
                        String afvalue = entry.getValue();
                        changeColunms.put(key.toLowerCase(), afvalue);
                    }
                }

                return Arrays.asList(
                        Message.builder()
                                .pk(canalMessage.getPk())
                                .dbName(canalMessage.getDatabase())
                                .tableName(canalMessage.getTable())
                                .type(opType.name())
                                .mode(canalMessage.getMode())
                                .db_table(
                                        canalMessage.getDatabase() + "." + canalMessage.getTable())
                                .data(toStringMap(data))
                                .es(canalMessage.getEs())
                                .ts(canalMessage.getTs())
                                .ps(Instant.now().toEpochMilli())
                                .build());
            });

    private String type;

    private Function<String, List<Message>> translate;

    private static final Pattern pattern =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}:\\d{2}:\\d{2}:\\d{2}");

    CollectType(String type, Function<String, List<Message>> translate) {
        this.type = type;
        this.translate = translate;
    }

    public static String toString(Object obj) {
        if (Objects.isNull(obj)) return null;
        String objStr = String.valueOf(obj);
        if (pattern.matcher(objStr).matches()) // 如果匹配到发现是这种格式，那么转换成标准格式
        return objStr.replaceFirst(":", " ");
        return objStr;
    }

    private static Map<String, String> toStringMap(Map<String, String> source) {

        return source.entrySet().stream()
                .collect(
                        HashMap::new,
                        (n, o) -> n.put(o.getKey().toLowerCase(), toString(o.getValue())),
                        HashMap::putAll);
    }

    public static CollectType getCollectType(String type) {
        for (CollectType collectType : CollectType.values()) {
            if (collectType.type == type) return collectType;
        }
        return CDC_MYSQL;
    }
}
