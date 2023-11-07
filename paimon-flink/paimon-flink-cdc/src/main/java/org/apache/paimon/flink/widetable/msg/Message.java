package org.apache.paimon.flink.widetable.msg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Message implements Serializable, Comparable {

    private String dbName;
    private String tableName;
    private String db_table;
    private String type;
    private String mode;
    private Map<String, String> data;
    private Long es; // 数据变更时间
    private Long ts; // canal处理时间
    private Long ps; // flink处理日志时间
    private String pk; // 主键值
    private String pk_col_name; // 主键字段名
    private Integer hash_pk; // 主键hash值

    private Map<String, List<Message>> resultListMap;

    @Override
    public int compareTo(Object o) {
        Message s = (Message) o;
        if (this.es > s.es) {
            return 1;
        } else if (this.es < s.es) {
            return -1;
        } else if (this.ps > s.ps) {
            return 1;
        } else if (this.ps < s.ps) {
            return -1;
        }
        return 0;
    }

    public void setReduceList(ArrayList<Object> objects) {}
}
