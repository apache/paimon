package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

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
            case INTEGER:
                sum = sum.add(BigDecimal.valueOf((int) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).intValue();
                break;
            case SMALLINT:
                sum = sum.add(BigDecimal.valueOf((short) inputField));
                average = sum.divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP).shortValue();
                break;
            default:
                throw new IllegalArgumentException();
        }


        return average;
    }
}
