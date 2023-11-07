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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDb_table() {
        return db_table;
    }

    public void setDb_table(String db_table) {
        this.db_table = db_table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getPs() {
        return ps;
    }

    public void setPs(Long ps) {
        this.ps = ps;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getPk_col_name() {
        return pk_col_name;
    }

    public void setPk_col_name(String pk_col_name) {
        this.pk_col_name = pk_col_name;
    }

    public Integer getHash_pk() {
        return hash_pk;
    }

    public void setHash_pk(Integer hash_pk) {
        this.hash_pk = hash_pk;
    }

    public Map<String, List<Message>> getResultListMap() {
        return resultListMap;
    }

    public void setResultListMap(Map<String, List<Message>> resultListMap) {
        this.resultListMap = resultListMap;
    }
}
