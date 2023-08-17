package org.apache.paimon.flink.source.operator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** this is a doc. */
public class ReadOperator2 extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<Tuple2<Split, String>, RowData> {

    private static final long serialVersionUID = 1L;

    private final List<ReadBuilder> readBuilders;

    //    private transient List<TableRead> reads;
    private transient Map<String, TableRead> readsMap;
    private transient StreamRecord<RowData> reuseRecord;
    private transient FlinkRowData reuseRow;

    public ReadOperator2(List<ReadBuilder> readBuilders) {
        this.readBuilders = readBuilders;
    }

    @Override
    public void open() throws Exception {
        super.open();
        IOManagerImpl ioManager =
                new IOManagerImpl(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        //        this.reads =
        //                readBuilders.stream()
        //                        .map(readBuilder ->
        // readBuilder.newRead().withIOManager(ioManager))
        //                        .collect(Collectors.toList());
        readsMap = new HashMap<>();
        for (ReadBuilder readBuilder : readBuilders) {
            readsMap.put(readBuilder.tableName(), readBuilder.newRead().withIOManager(ioManager));
        }
        this.reuseRow = new FlinkRowData(null);
        this.reuseRecord = new StreamRecord<>(reuseRow);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Split, String>> record) throws Exception {
        // 只要和当前split匹配的reader，想办法把tableName传进来
        TableRead read = readsMap.get(record.getValue().f1);
        try (CloseableIterator<InternalRow> iterator =
                read.createReader(record.getValue().f0).toCloseableIterator()) {
            while (iterator.hasNext()) {
                reuseRow.replace(iterator.next());
                output.collect(reuseRecord);
            }
        }
    }
}
