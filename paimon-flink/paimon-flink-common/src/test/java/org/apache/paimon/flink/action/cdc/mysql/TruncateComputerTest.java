package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(DataProviderRunner.class)
public class TruncateComputerTest {

    @DataProvider
    public static Object[][] provideTestGetTruncateCaseData() {
        return new Object[][] {
            {"computedColumnField", "0", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "1", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "5", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "9", new TinyIntType(true), "10", "0"},
            {"computedColumnField", "10", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "11", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "15", new TinyIntType(true), "10", "10"},
            {"computedColumnField", "-1", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new TinyIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new TinyIntType(true), "10", "-20"},
            {"computedColumnField", "0", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "1", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "5", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "9", new SmallIntType(true), "10", "0"},
            {"computedColumnField", "10", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "11", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "15", new SmallIntType(true), "10", "10"},
            {"computedColumnField", "-1", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new SmallIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new SmallIntType(true), "10", "-20"},
            {"computedColumnField", "0", new IntType(true), "10", "0"},
            {"computedColumnField", "1", new IntType(true), "10", "0"},
            {"computedColumnField", "5", new IntType(true), "10", "0"},
            {"computedColumnField", "9", new IntType(true), "10", "0"},
            {"computedColumnField", "10", new IntType(true), "10", "10"},
            {"computedColumnField", "11", new IntType(true), "10", "10"},
            {"computedColumnField", "15", new IntType(true), "10", "10"},
            {"computedColumnField", "-1", new IntType(true), "10", "-10"},
            {"computedColumnField", "-5", new IntType(true), "10", "-10"},
            {"computedColumnField", "-9", new IntType(true), "10", "-10"},
            {"computedColumnField", "-10", new IntType(true), "10", "-10"},
            {"computedColumnField", "-11", new IntType(true), "10", "-20"},
            {"computedColumnField", "0", new BigIntType(true), "10", "0"},
            {"computedColumnField", "1", new BigIntType(true), "10", "0"},
            {"computedColumnField", "5", new BigIntType(true), "10", "0"},
            {"computedColumnField", "9", new BigIntType(true), "10", "0"},
            {"computedColumnField", "10", new BigIntType(true), "10", "10"},
            {"computedColumnField", "11", new BigIntType(true), "10", "10"},
            {"computedColumnField", "15", new BigIntType(true), "10", "10"},
            {"computedColumnField", "-1", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-5", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-9", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-10", new BigIntType(true), "10", "-10"},
            {"computedColumnField", "-11", new BigIntType(true), "10", "-20"},
            {"computedColumnField", "12.34", new DecimalType(9, 2), "10", "12.30"},
            {"computedColumnField", "12.30", new DecimalType(9, 2), "10", "12.30"},
            {"computedColumnField", "12.29", new DecimalType(9, 2), "10", "12.20"},
            {"computedColumnField", "0.05", new DecimalType(9, 2), "10", "0.00"},
            {"computedColumnField", "-0.05", new DecimalType(9, 2), "10", "-0.10"},
            {"computedColumnField", "abcde", new VarCharType(true, 5), "3", "abc"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "3", "abc"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "5", "abcde"},
            {"computedColumnField", "abcdefg", new VarCharType(true, 7), "7", "abcdefg"},
            {"computedColumnField", "abcde", new CharType(true, 5), "3", "abc"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "3", "abc"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "5", "abcde"},
            {"computedColumnField", "abcdefg", new CharType(true, 7), "7", "abcdefg"},
        };
    }

    @Test
    @UseDataProvider("provideTestGetTruncateCaseData")
    public void testTruncate(
            String fieldReference,
            String value,
            DataType dataType,
            String literal,
            String expected) {
        TruncateComputer truncateComputer = new TruncateComputer(fieldReference, dataType, literal);
        assertThat(truncateComputer.eval(value)).isEqualTo(expected);
    }

    @Test
    public void testTruncateWithException() {
        String fieldReference = "computedColumnField";
        DataType dataType = new CharType(true, 5);
        TruncateComputer truncateComputer = new TruncateComputer(fieldReference, dataType, "7");

        assertThatThrownBy(() -> truncateComputer.eval("abcde"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid width value for truncate function: 7, expected less than or equal to 5.");

        DataType notSupportedDataType = new BooleanType();
        TruncateComputer notSupportTruncateComputer =
                new TruncateComputer(fieldReference, notSupportedDataType, "7");
        assertThatThrownBy(() -> notSupportTruncateComputer.eval("true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported field type for truncate function: BOOLEAN");
    }
}
