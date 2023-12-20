package org.apache.paimon.spark.sort;

import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.spark.sql.functions.array;

public class HilbertSorter extends TableSorter {

    private static final String H_COLUMN = "HVALUE";

    public HilbertSorter(FileStoreTable table, List<String> orderColumns) {
        super(table, orderColumns);
        checkNotEmpty();
    }

    @Override
    public Dataset<Row> sort(Dataset<Row> df) {
        Column hilbertColumn = hilbertValue(df);
        Dataset<Row> hilbertValueDF = df.withColumn(H_COLUMN, hilbertColumn);
        Dataset<Row> sortedDF =
                hilbertValueDF
                        .repartitionByRange(hilbertValueDF.col(H_COLUMN))
                        .sortWithinPartitions(hilbertValueDF.col(H_COLUMN));
        return sortedDF.drop(H_COLUMN);
    }

    private Column hilbertValue(Dataset<Row> df) {
        SparkHilbertUDF hilbertUDF = new SparkHilbertUDF();

        Column[] hilbertCols =
                orderColNames.stream()
                        .map(df.schema()::apply)
                        .map(
                                col ->
                                        hilbertUDF.sortedLexicographically(
                                                df.col(col.name()), col.dataType()))
                        .toArray(Column[]::new);

        return hilbertUDF.transform(array(hilbertCols));
    }
}
