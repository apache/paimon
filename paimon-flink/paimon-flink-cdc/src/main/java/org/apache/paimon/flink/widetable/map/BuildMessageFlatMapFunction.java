package org.apache.paimon.flink.widetable.map;

import org.apache.paimon.flink.widetable.msg.CollectType;
import org.apache.paimon.flink.widetable.msg.Message;
import org.apache.paimon.flink.widetable.utils.Constant;
import org.apache.paimon.flink.widetable.utils.ParamUitl;
import org.apache.paimon.flink.widetable.utils.options.JobOptions;
import org.apache.paimon.flink.widetable.utils.options.SourceOptions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Map;

public class BuildMessageFlatMapFunction extends RichFlatMapFunction<String, Message> {
    private static final long serialVersionUID = 1L;

    private static transient CollectType collectType = CollectType.getCollectType("mysql");
    /** hash 并行* */
    private static transient int hash_parallel;
    /** 主键映射 * */
    private static transient Map<String, String> kpMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            ExecutionConfig.GlobalJobParameters globalParams =
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Configuration globConf = (Configuration) globalParams;

            this.hash_parallel = globConf.getInteger(JobOptions.PARALLEL);
            String dbtables = globConf.getString(SourceOptions.INCLUDING_TABLES);
            this.kpMap = ParamUitl.praseKpMap(dbtables);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get hash_parallel.", e);
        }
    }

    @Override
    public void flatMap(String value, Collector<Message> list) throws Exception {
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask(); // 子任务id/分区编号
        try {
            this.collectType
                    .getTranslate()
                    .apply(value)
                    .forEach(
                            item -> {
                                buildHash_Pk(subTaskId, item);
                                list.collect(item);
                            });
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void buildHash_Pk(int subTaskId, Message item) {
        buidPk(item);
        int subtaskIndex = Integer.parseInt(item.getPk()) % this.hash_parallel;
        item.setHash_pk(subtaskIndex);
    }

    private void buidPk(Message item) {

        String pkName = Constant.PK;
        String db_table = item.getDb_table();
        if (kpMap.containsKey(db_table)) {
            pkName = kpMap.get(db_table);
        }
        String kpValue = item.getData().get(pkName);
        item.setPk(kpValue);
        item.setPk_col_name(pkName);
    }
}
