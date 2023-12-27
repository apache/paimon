package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** avg aggregate a field of a row. */
public class FieldAvgAgg extends FieldAggregator {

    public static final String NAME = "avg";
    private int count;
    private BigDecimal sum;

    public FieldAvgAgg(DataType dataType) {
        super(dataType);
        sum = BigDecimal.ZERO;
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        Object average;
        if (inputField == null) {
            return accumulator;
        }
        count++;
        switch (fieldType.getTypeRoot()) {
            case DECIMAL:
                sum = sum.add((BigDecimal) inputField);
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP);
                break;
            case TINYINT:
                sum = sum.add(BigDecimal.valueOf((byte) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).byteValue();
                break;
            case SMALLINT:
                sum = sum.add(BigDecimal.valueOf((short) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).shortValue();
                break;
            case INTEGER:
                sum = sum.add(BigDecimal.valueOf((int) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).intValue();
                break;
            case BIGINT:
                sum = sum.add(BigDecimal.valueOf((long) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).longValue();
                break;
            case FLOAT:
                sum = sum.add(BigDecimal.valueOf((float) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).floatValue();
                break;
            case DOUBLE:
                sum = sum.add(BigDecimal.valueOf((double) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).doubleValue();
                break;
            default:
                throw new IllegalArgumentException();
        }

        return average;
    }
}
