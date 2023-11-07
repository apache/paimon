package org.apache.paimon.flink.widetable.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionWindowReduceFunction implements ReduceFunction<Message> {

    @Override
    public Message reduce(Message value1, Message value2) throws Exception {
        Message newMessage =
                Message.builder()
                        .pk(value2.getPk())
                        .pk_col_name(value2.getPk_col_name())
                        .hash_pk(value2.getHash_pk())
                        .dbName(value2.getDbName())
                        .db_table(value2.getDb_table())
                        .tableName(value2.getTableName())
                        .type(value2.getType())
                        .db_table(value2.getDb_table())
                        .data(value2.getData())
                        .es(value2.getEs())
                        .ts(value2.getTs())
                        .ps(value2.getPs())
                        .build();

        Map<String, List<Message>> reduceListMap1 = value1.getResultListMap();
        if (reduceListMap1 == null) {
            Map<String, List<Message>> reduceListMapNew = new HashMap<>();
            List<Message> list = new ArrayList<>();
            list.add(value1);
            reduceListMapNew.put(value1.getDb_table(), list);
            newMessage.setResultListMap(reduceListMapNew);

        } else {
            newMessage.setResultListMap(reduceListMap1);
        }

        if (!newMessage.getResultListMap().containsKey(value2.getDb_table())) {
            List<Message> list = new ArrayList<>();
            list.add(value2);
            newMessage.getResultListMap().put(value2.getDb_table(), list);
        } else {
            newMessage.getResultListMap().get(value2.getDb_table()).add(value2);
        }

        return newMessage;
    }
}
