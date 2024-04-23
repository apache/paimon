/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.utils.SortUtil;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.data.BinaryString.EMPTY_UTF8;
import static org.apache.paimon.data.BinaryString.blankString;
import static org.apache.paimon.data.BinaryString.fromBytes;
import static org.apache.paimon.utils.DecimalUtils.castFrom;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test of {@link BinaryString}.
 *
 * <p>Caution that you must construct a string by {@link #fromString} to cover all the test cases.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class BinaryStringTest {

    private final Mode mode;

    public BinaryStringTest(Mode mode) {
        this.mode = mode;
    }

    @SuppressWarnings("unused")
    @Parameters(name = "{0}")
    public static List<Mode> getVarSeg() {
        return Arrays.asList(Mode.ONE_SEG, Mode.MULTI_SEGS, Mode.STRING, Mode.RANDOM);
    }

    private enum Mode {
        ONE_SEG,
        MULTI_SEGS,
        STRING,
        RANDOM
    }

    private BinaryString fromString(String str) {
        BinaryString string = BinaryString.fromString(str);

        Mode mode = this.mode;

        if (mode == Mode.RANDOM) {
            int rnd = new Random().nextInt(3);
            if (rnd == 0) {
                mode = Mode.ONE_SEG;
            } else if (rnd == 1) {
                mode = Mode.MULTI_SEGS;
            } else {
                mode = Mode.STRING;
            }
        }

        if (mode == Mode.STRING) {
            return string;
        }
        if (mode == Mode.ONE_SEG || string.getSizeInBytes() < 2) {
            return string;
        } else {
            int numBytes = string.getSizeInBytes();
            int pad = new Random().nextInt(5);
            int numBytesWithPad = numBytes + pad;
            int segSize = numBytesWithPad / 2 + 1;
            byte[] bytes1 = new byte[segSize];
            byte[] bytes2 = new byte[segSize];
            if (segSize - pad > 0 && numBytes >= segSize - pad) {
                string.getSegments()[0].get(0, bytes1, pad, segSize - pad);
            }
            string.getSegments()[0].get(segSize - pad, bytes2, 0, numBytes - segSize + pad);
            return BinaryString.fromAddress(
                    new MemorySegment[] {MemorySegment.wrap(bytes1), MemorySegment.wrap(bytes2)},
                    pad,
                    numBytes);
        }
    }

    private void checkBasic(String str, int len) {
        BinaryString s1 = fromString(str);
        BinaryString s2 = fromBytes(str.getBytes(StandardCharsets.UTF_8));
        assertThat(len).isEqualTo(s1.numChars());
        assertThat(len).isEqualTo(s2.numChars());

        assertThat(str).isEqualTo(s1.toString());
        assertThat(str).isEqualTo(s2.toString());
        assertThat(s2).isEqualTo(s1);

        assertThat(s2.hashCode()).isEqualTo(s1.hashCode());

        assertThat(s1.compareTo(s2)).isEqualTo(0);

        assertThat(s1.contains(s2)).isTrue();
        assertThat(s2.contains(s1)).isTrue();
        assertThat(s1.startsWith(s1)).isTrue();
        assertThat(s1.endsWith(s1)).isTrue();
    }

    @TestTemplate
    public void basicTest() {
        checkBasic("", 0);
        checkBasic(",", 1);
        checkBasic("hello", 5);
        checkBasic("hello world", 11);
        checkBasic("Paimon中文社区", 10);
        checkBasic("中 文 社 区", 7);

        checkBasic("¡", 1); // 2 bytes char
        checkBasic("ку", 2); // 2 * 2 bytes chars
        checkBasic("︽﹋％", 3); // 3 * 3 bytes chars
        checkBasic("\uD83E\uDD19", 1); // 4 bytes char
    }

    @TestTemplate
    public void emptyStringTest() {
        assertThat(fromString("")).isEqualTo(EMPTY_UTF8);
        assertThat(fromBytes(new byte[0])).isEqualTo(EMPTY_UTF8);
        assertThat(EMPTY_UTF8.numChars()).isEqualTo(0);
        assertThat(EMPTY_UTF8.getSizeInBytes()).isEqualTo(0);
    }

    @TestTemplate
    public void compareTo() {
        assertThat(fromString("   ").compareTo(blankString(3))).isEqualTo(0);
        assertThat(fromString("").compareTo(fromString("a"))).isLessThan(0);
        assertThat(fromString("abc").compareTo(fromString("ABC"))).isGreaterThan(0);
        assertThat(fromString("abc0").compareTo(fromString("abc"))).isGreaterThan(0);
        assertThat(fromString("abcabcabc").compareTo(fromString("abcabcabc"))).isEqualTo(0);
        assertThat(fromString("aBcabcabc").compareTo(fromString("Abcabcabc"))).isGreaterThan(0);
        assertThat(fromString("Abcabcabc").compareTo(fromString("abcabcabC"))).isLessThan(0);
        assertThat(fromString("abcabcabc").compareTo(fromString("abcabcabC"))).isGreaterThan(0);

        assertThat(fromString("abc").compareTo(fromString("世界"))).isLessThan(0);
        assertThat(fromString("你好").compareTo(fromString("世界"))).isGreaterThan(0);
        assertThat(fromString("你好123").compareTo(fromString("你好122"))).isGreaterThan(0);

        MemorySegment segment1 = MemorySegment.allocateHeapMemory(1024);
        MemorySegment segment2 = MemorySegment.allocateHeapMemory(1024);
        SortUtil.putStringNormalizedKey(fromString("abcabcabc"), segment1, 0, 9);
        SortUtil.putStringNormalizedKey(fromString("abcabcabC"), segment2, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isGreaterThan(0);
        SortUtil.putStringNormalizedKey(fromString("abcab"), segment1, 0, 9);
        assertThat(segment1.compare(segment2, 0, 0, 9)).isLessThan(0);
    }

    @TestTemplate
    public void testMultiSegments() {

        // prepare
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[10]);
        segments1[1] = MemorySegment.wrap(new byte[10]);
        segments1[0].put(5, "abcde".getBytes(UTF_8), 0, 5);
        segments1[1].put(0, "aaaaa".getBytes(UTF_8), 0, 5);

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegment.wrap(new byte[5]);
        segments2[1] = MemorySegment.wrap(new byte[5]);
        segments2[0].put(0, "abcde".getBytes(UTF_8), 0, 5);
        segments2[1].put(0, "b".getBytes(UTF_8), 0, 1);

        // test go ahead both
        BinaryString binaryString1 = BinaryString.fromAddress(segments1, 5, 10);
        BinaryString binaryString2 = BinaryString.fromAddress(segments2, 0, 6);
        assertThat(binaryString1.toString()).isEqualTo("abcdeaaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcdeb");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);

        // test needCompare == len
        binaryString1 = BinaryString.fromAddress(segments1, 5, 5);
        binaryString2 = BinaryString.fromAddress(segments2, 0, 5);
        assertThat(binaryString1.toString()).isEqualTo("abcde");
        assertThat(binaryString2.toString()).isEqualTo("abcde");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(0);

        // test find the first segment of this string
        binaryString1 = BinaryString.fromAddress(segments1, 10, 5);
        binaryString2 = BinaryString.fromAddress(segments2, 0, 5);
        assertThat(binaryString1.toString()).isEqualTo("aaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcde");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
        assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);

        // test go ahead single
        segments2 = new MemorySegment[] {MemorySegment.wrap(new byte[10])};
        segments2[0].put(4, "abcdeb".getBytes(UTF_8), 0, 6);
        binaryString1 = BinaryString.fromAddress(segments1, 5, 10);
        binaryString2 = BinaryString.fromAddress(segments2, 4, 6);
        assertThat(binaryString1.toString()).isEqualTo("abcdeaaaaa");
        assertThat(binaryString2.toString()).isEqualTo("abcdeb");
        assertThat(binaryString1.compareTo(binaryString2)).isEqualTo(-1);
        assertThat(binaryString2.compareTo(binaryString1)).isEqualTo(1);
    }

    @TestTemplate
    public void contains() {
        assertThat(EMPTY_UTF8.contains(EMPTY_UTF8)).isTrue();
        assertThat(fromString("hello").contains(fromString("ello"))).isTrue();
        assertThat(fromString("hello").contains(fromString("vello"))).isFalse();
        assertThat(fromString("hello").contains(fromString("hellooo"))).isFalse();
        assertThat(fromString("大千世界").contains(fromString("千世界"))).isTrue();
        assertThat(fromString("大千世界").contains(fromString("世千"))).isFalse();
        assertThat(fromString("大千世界").contains(fromString("大千世界好"))).isFalse();
    }

    @TestTemplate
    public void startsWith() {
        assertThat(EMPTY_UTF8.startsWith(EMPTY_UTF8)).isTrue();
        assertThat(fromString("hello").startsWith(fromString("hell"))).isTrue();
        assertThat(fromString("hello").startsWith(fromString("ell"))).isFalse();
        assertThat(fromString("hello").startsWith(fromString("hellooo"))).isFalse();
        assertThat(fromString("数据砖头").startsWith(fromString("数据"))).isTrue();
        assertThat(fromString("大千世界").startsWith(fromString("千"))).isFalse();
        assertThat(fromString("大千世界").startsWith(fromString("大千世界好"))).isFalse();
    }

    @TestTemplate
    public void endsWith() {
        assertThat(EMPTY_UTF8.endsWith(EMPTY_UTF8)).isTrue();
        assertThat(fromString("hello").endsWith(fromString("ello"))).isTrue();
        assertThat(fromString("hello").endsWith(fromString("ellov"))).isFalse();
        assertThat(fromString("hello").endsWith(fromString("hhhello"))).isFalse();
        assertThat(fromString("大千世界").endsWith(fromString("世界"))).isTrue();
        assertThat(fromString("大千世界").endsWith(fromString("世"))).isFalse();
        assertThat(fromString("数据砖头").endsWith(fromString("我的数据砖头"))).isFalse();
    }

    @TestTemplate
    public void substring() {
        assertThat(fromString("hello").substring(0, 0)).isEqualTo(EMPTY_UTF8);
        assertThat(fromString("hello").substring(1, 3)).isEqualTo(fromString("el"));
        assertThat(fromString("数据砖头").substring(0, 1)).isEqualTo(fromString("数"));
        assertThat(fromString("数据砖头").substring(1, 3)).isEqualTo(fromString("据砖"));
        assertThat(fromString("数据砖头").substring(3, 5)).isEqualTo(fromString("头"));
        assertThat(fromString("ߵ梷").substring(0, 2)).isEqualTo(fromString("ߵ梷"));
    }

    @TestTemplate
    public void indexOf() {
        assertThat(EMPTY_UTF8.indexOf(EMPTY_UTF8, 0)).isEqualTo(0);
        assertThat(EMPTY_UTF8.indexOf(fromString("l"), 0)).isEqualTo(-1);
        assertThat(fromString("hello").indexOf(EMPTY_UTF8, 0)).isEqualTo(0);
        assertThat(fromString("hello").indexOf(fromString("l"), 0)).isEqualTo(2);
        assertThat(fromString("hello").indexOf(fromString("l"), 3)).isEqualTo(3);
        assertThat(fromString("hello").indexOf(fromString("a"), 0)).isEqualTo(-1);
        assertThat(fromString("hello").indexOf(fromString("ll"), 0)).isEqualTo(2);
        assertThat(fromString("hello").indexOf(fromString("ll"), 4)).isEqualTo(-1);
        assertThat(fromString("数据砖头").indexOf(fromString("据砖"), 0)).isEqualTo(1);
        assertThat(fromString("数据砖头").indexOf(fromString("数"), 3)).isEqualTo(-1);
        assertThat(fromString("数据砖头").indexOf(fromString("数"), 0)).isEqualTo(0);
        assertThat(fromString("数据砖头").indexOf(fromString("头"), 0)).isEqualTo(3);
    }

    @TestTemplate
    public void testToUpperLowerCase() {
        assertThat(fromString("我是中国人").toLowerCase()).isEqualTo(fromString("我是中国人"));
        assertThat(fromString("我是中国人").toUpperCase()).isEqualTo(fromString("我是中国人"));

        assertThat(fromString("aBcDeFg").toLowerCase()).isEqualTo(fromString("abcdefg"));
        assertThat(fromString("aBcDeFg").toUpperCase()).isEqualTo(fromString("ABCDEFG"));

        assertThat(fromString("!@#$%^*").toLowerCase()).isEqualTo(fromString("!@#$%^*"));
        assertThat(fromString("!@#$%^*").toLowerCase()).isEqualTo(fromString("!@#$%^*"));
        // Test composite in BinaryRow.
        BinaryRow row = new BinaryRow(20);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString("a"));
        writer.writeString(1, BinaryString.fromString("我是中国人"));
        writer.writeString(3, BinaryString.fromString("aBcDeFg"));
        writer.writeString(5, BinaryString.fromString("!@#$%^*"));
        writer.complete();

        assertThat(row.getString(0).toUpperCase()).isEqualTo(fromString("A"));
        assertThat(row.getString(1).toUpperCase()).isEqualTo(fromString("我是中国人"));
        assertThat(row.getString(1).toLowerCase()).isEqualTo(fromString("我是中国人"));
        assertThat(row.getString(3).toUpperCase()).isEqualTo(fromString("ABCDEFG"));
        assertThat(row.getString(3).toLowerCase()).isEqualTo(fromString("abcdefg"));
        assertThat(row.getString(5).toUpperCase()).isEqualTo(fromString("!@#$%^*"));
        assertThat(row.getString(5).toLowerCase()).isEqualTo(fromString("!@#$%^*"));
    }

    @TestTemplate
    public void testCastFrom() {
        class DecimalTestData {
            private final String str;
            private final int precision;
            private final int scale;

            private DecimalTestData(String str, int precision, int scale) {
                this.str = str;
                this.precision = precision;
                this.scale = scale;
            }
        }

        DecimalTestData[] data = {
            new DecimalTestData("12.345", 5, 3),
            new DecimalTestData("-12.345", 5, 3),
            new DecimalTestData("+12345", 5, 0),
            new DecimalTestData("-12345", 5, 0),
            new DecimalTestData("12345.", 5, 0),
            new DecimalTestData("-12345.", 5, 0),
            new DecimalTestData(".12345", 5, 5),
            new DecimalTestData("-.12345", 5, 5),
            new DecimalTestData("+12.345E3", 5, 0),
            new DecimalTestData("-12.345e3", 5, 0),
            new DecimalTestData("12.345e-3", 6, 6),
            new DecimalTestData("-12.345E-3", 6, 6),
            new DecimalTestData("12345E3", 8, 0),
            new DecimalTestData("-12345e3", 8, 0),
            new DecimalTestData("12345e-3", 5, 3),
            new DecimalTestData("-12345E-3", 5, 3),
            new DecimalTestData("+.12345E3", 5, 2),
            new DecimalTestData("-.12345e3", 5, 2),
            new DecimalTestData(".12345e-3", 8, 8),
            new DecimalTestData("-.12345E-3", 8, 8),
            new DecimalTestData("1234512345.1234", 18, 8),
            new DecimalTestData("-1234512345.1234", 18, 8),
            new DecimalTestData("1234512345.1234", 12, 2),
            new DecimalTestData("-1234512345.1234", 12, 2),
            new DecimalTestData("1234512345.1299", 12, 2),
            new DecimalTestData("-1234512345.1299", 12, 2),
            new DecimalTestData("999999999999999999", 18, 0),
            new DecimalTestData("1234512345.1234512345", 20, 10),
            new DecimalTestData("-1234512345.1234512345", 20, 10),
            new DecimalTestData("1234512345.1234512345", 15, 5),
            new DecimalTestData("-1234512345.1234512345", 15, 5),
            new DecimalTestData("12345123451234512345E-10", 20, 10),
            new DecimalTestData("-12345123451234512345E-10", 20, 10),
            new DecimalTestData("12345123451234512345E-10", 15, 5),
            new DecimalTestData("-12345123451234512345E-10", 15, 5),
            new DecimalTestData("999999999999999999999", 21, 0),
            new DecimalTestData("-999999999999999999999", 21, 0),
            new DecimalTestData("0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 38, 38),
            new DecimalTestData("0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("-0.00000000000000000000123456789123456789", 29, 29),
            new DecimalTestData("123456789123E-27", 18, 18),
            new DecimalTestData("-123456789123E-27", 18, 18),
            new DecimalTestData("123456789999E-27", 18, 18),
            new DecimalTestData("-123456789999E-27", 18, 18),
            new DecimalTestData("123456789123456789E-36", 18, 18),
            new DecimalTestData("-123456789123456789E-36", 18, 18),
            new DecimalTestData("123456789999999999E-36", 18, 18),
            new DecimalTestData("-123456789999999999E-36", 18, 18)
        };

        for (DecimalTestData d : data) {
            assertThat(castFrom(fromString(d.str), d.precision, d.scale))
                    .isEqualTo(Decimal.fromBigDecimal(new BigDecimal(d.str), d.precision, d.scale));
        }

        BinaryRow row = new BinaryRow(data.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < data.length; i++) {
            writer.writeString(i, BinaryString.fromString(data[i].str));
        }
        writer.complete();
        for (int i = 0; i < data.length; i++) {
            DecimalTestData d = data[i];
            assertThat(castFrom(row.getString(i), d.precision, d.scale))
                    .isEqualTo(Decimal.fromBigDecimal(new BigDecimal(d.str), d.precision, d.scale));
        }
    }

    @TestTemplate
    public void testEmptyString() {
        BinaryString str2 = fromString("hahahahah");
        BinaryString str3;
        {
            MemorySegment[] segments = new MemorySegment[2];
            segments[0] = MemorySegment.wrap(new byte[10]);
            segments[1] = MemorySegment.wrap(new byte[10]);
            str3 = BinaryString.fromAddress(segments, 15, 0);
        }

        assertThat(EMPTY_UTF8.compareTo(str2)).isLessThan(0);
        assertThat(str2.compareTo(EMPTY_UTF8)).isGreaterThan(0);

        assertThat(EMPTY_UTF8.compareTo(str3)).isEqualTo(0);
        assertThat(str3.compareTo(EMPTY_UTF8)).isEqualTo(0);

        assertThat(str2).isNotEqualTo(EMPTY_UTF8);
        assertThat(EMPTY_UTF8).isNotEqualTo(str2);

        assertThat(str3).isEqualTo(EMPTY_UTF8);
        assertThat(EMPTY_UTF8).isEqualTo(str3);
    }

    @TestTemplate
    public void testEncodeWithIllegalCharacter() {

        // Tis char array has some illegal character, such as 55357
        // the jdk ignores theses character and cast them to '?'
        // which StringUtf8Utils'encodeUTF8 should follow
        char[] chars =
                new char[] {
                    20122, 40635, 124, 38271, 34966, 124, 36830, 34915, 35033, 124, 55357, 124,
                    56407
                };

        String str = new String(chars);

        assertThat(BinaryString.encodeUTF8(str)).isEqualTo(str.getBytes(UTF_8));
    }

    @TestTemplate
    public void testDecodeWithIllegalUtf8Bytes() {

        // illegal utf-8 bytes
        byte[] bytes =
                new byte[] {
                    (byte) 20122,
                    (byte) 40635,
                    124,
                    (byte) 38271,
                    (byte) 34966,
                    124,
                    (byte) 36830,
                    (byte) 34915,
                    (byte) 35033,
                    124,
                    (byte) 55357,
                    124,
                    (byte) 56407
                };

        String str = new String(bytes, StandardCharsets.UTF_8);
        assertThat(BinaryString.decodeUTF8(bytes, 0, bytes.length)).isEqualTo(str);
        assertThat(BinaryString.decodeUTF8(MemorySegment.wrap(bytes), 0, bytes.length))
                .isEqualTo(str);

        byte[] newBytes = new byte[bytes.length + 5];
        System.arraycopy(bytes, 0, newBytes, 5, bytes.length);
        assertThat(BinaryString.decodeUTF8(MemorySegment.wrap(newBytes), 5, bytes.length))
                .isEqualTo(str);
    }

    @TestTemplate
    public void skipWrongFirstByte() {
        int[] wrongFirstBytes = {
            0x80,
            0x9F,
            0xBF, // Skip Continuation bytes
            0xC0,
            0xC2, // 0xC0..0xC1 - disallowed in UTF-8
            // 0xF5..0xFF - disallowed in UTF-8
            0xF5,
            0xF6,
            0xF7,
            0xF8,
            0xF9,
            0xFA,
            0xFB,
            0xFC,
            0xFD,
            0xFE,
            0xFF
        };
        byte[] c = new byte[1];

        for (int wrongFirstByte : wrongFirstBytes) {
            c[0] = (byte) wrongFirstByte;
            assertThat(1).isEqualTo(fromBytes(c).numChars());
        }
    }

    @TestTemplate
    public void testFromBytes() {
        String s = "hahahe";
        byte[] bytes = Arrays.copyOf(s.getBytes(UTF_8), 10);
        assertThat(BinaryString.fromBytes(bytes, 0, 6)).isEqualTo(BinaryString.fromString(s));
    }
}
