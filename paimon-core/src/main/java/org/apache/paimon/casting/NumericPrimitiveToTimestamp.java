package org.apache.paimon.casting;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.time.ZoneId;

public class NumericPrimitiveToTimestamp extends AbstractCastRule<Number, Timestamp> {

    static final NumericPrimitiveToTimestamp INSTANCE = new NumericPrimitiveToTimestamp();

    private NumericPrimitiveToTimestamp() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.NUMERIC)
                        .target(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<Number, Timestamp> create(DataType inputType, DataType targetType) {
        if (targetType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            switch (inputType.getTypeRoot()) {
                case INTEGER:
                case BIGINT:
                    return value -> Timestamp.fromLocalDateTime(DateTimeUtils.toLocalDateTime(value.longValue(), ZoneId.systemDefault()));
                default:
                    return null;
            }
        } else {
            switch (inputType.getTypeRoot()) {
                case INTEGER:
                case BIGINT:
                    return value -> Timestamp.fromLocalDateTime(DateTimeUtils.toLocalDateTime(value.longValue(), DateTimeUtils.UTC_ZONE.toZoneId()));
                default:
                    return null;
            }
        }
    }
}
