package org.apache.paimon.flink.widetable.map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageMysqlSinkMapFunction extends RichMapFunction<Message, Message> {
    private static final long serialVersionUID = 1L;

    private transient Connection connection;

    private static transient Map<String, DimensionTable> tablesMap;

    @Override
    public void open(Configuration parameters) throws Exception {

        try {
            ExecutionConfig.GlobalJobParameters globalParams =
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Configuration globConf = (Configuration) globalParams;
            this.tablesMap = DimensionTableHelper.buildTableMap(globConf);

            if (null == connection) {
                connection = DimensionTableHelper.getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot create connection to Mysql.", e);
        }
        System.out.println("mysql sink connection open");
    }

    @Override
    public Message map(Message windowMsg) throws Exception {

        PreparedStatement p = null;
        try {
            int subTaskId = getRuntimeContext().getIndexOfThisSubtask(); // 子任务id/分区编号
            Map<String, List<Message>> resultListMap = windowMsg.getResultListMap();

            if (resultListMap == null) { // 单条记录时不走reduce
                resultListMap = new HashMap<>();
                List<Message> list = new ArrayList<>();
                list.add(windowMsg);
                resultListMap.put(windowMsg.getDb_table(), list);
            }

            connection.setAutoCommit(false);
            for (Map.Entry<String, List<Message>> r : resultListMap.entrySet()) {
                String db_table = r.getKey();
                String tableName = db_table.replace(".", "_");
                List<Message> messageList = r.getValue();
                // 排序
                Collections.sort(messageList);
                DimensionTable ztable = tablesMap.get(tableName);

                StringBuffer replaceSql =
                        DimensionTableHelper.buildReplaceSql(
                                ztable.getTable_name(), ztable.buildColumns());
                p = connection.prepareStatement(replaceSql.toString());

                long start = System.currentTimeMillis();
                for (Message value : messageList) {

                    Map<String, String> data = value.getData();
                    String type = value.getType();
                    OpType opType = OpType.getOpType(type);

                    switch (opType) {
                        case INSERT:
                            DimensionTableHelper.buildData(ztable, p, data, false);
                            break;
                        case UPDATE:
                            DimensionTableHelper.buildData(ztable, p, data, false);
                            break;
                        case DELETE:
                            DimensionTableHelper.buildData(ztable, p, data, true);
                            break;
                    }
                    p.addBatch();
                }
                p.executeBatch();
                connection.commit();

                long end = System.currentTimeMillis();
                System.out.println(
                        "MysqlSinkMap --> subTaskId:"
                                + subTaskId
                                + ",table:"
                                + tableName
                                + ",msg_count:"
                                + messageList.size()
                                + ",usetime:"
                                + (end - start)
                                + ",timestamp:"
                                + end);
                log.info(
                        "MysqlSinkMap --> subTaskId:"
                                + subTaskId
                                + ",table:"
                                + tableName
                                + ",msg_count:"
                                + messageList.size()
                                + ",usetime:"
                                + (end - start)
                                + ",timestamp:"
                                + end);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("error " + e.getMessage());
            throw new RuntimeException("MessageMysqlSinkMapFunction >> update Mysql", e);
        } finally {
            if (p != null) {
                p.close();
            }
        }
        return windowMsg;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        System.out.println("mysql sink connection close");
    }
}
