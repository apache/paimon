package org.apache.paimon.reader;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;

/** dsds. */
public class ExcelWriteStrategy implements WriteStrategy {

    @Override
    public void writer(BatchTableWrite batchTableWrite, String content, String columnSeparator)
            throws Exception {}

    @Override
    public Schema retrieveSchema() throws Exception {
        return null;
    }
}
