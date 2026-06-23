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

package org.apache.paimon.data.shredding;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSharedShreddingRowConverter}. */
class MapSharedShreddingRowConverterTest {

    @Test
    void testBasicConversion() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("tags", 3));
        assertThat(converter.shreddingFieldNames()).containsExactly("tags");
        assertThatThrownBy(() -> converter.shreddingFieldNames().add("metrics"))
                .isInstanceOf(UnsupportedOperationException.class);

        InternalRow first = converter.convert(GenericRow.of(100, stringKeyMap("a", 1L, "b", 2L)));
        InternalRow firstTags = first.getRow(1, 5);

        // Target physical row:
        // id = 100
        // tags = [__field_mapping=[a, b, empty], __col_0=1, __col_1=2, __col_2=null,
        //         __overflow=null]
        assertThat(first.getInt(0)).isEqualTo(100);
        assertThat(firstTags.getArray(0).toIntArray()).containsExactly(0, 1, -1);
        assertThat(firstTags.getLong(1)).isEqualTo(1L);
        assertThat(firstTags.getLong(2)).isEqualTo(2L);
        assertThat(firstTags.isNullAt(3)).isTrue();
        assertThat(firstTags.isNullAt(4)).isTrue();

        InternalRow second =
                converter.convert(GenericRow.of(200, stringKeyMap("b", 3L, "c", 4L, "a", 5L)));
        InternalRow secondTags = second.getRow(1, 5);

        // Target physical row:
        // id = 200
        // tags = [__field_mapping=[b, c, a], __col_0=3, __col_1=4, __col_2=5,
        //         __overflow=null]
        assertThat(second.getInt(0)).isEqualTo(200);
        assertThat(secondTags.getArray(0).toIntArray()).containsExactly(1, 2, 0);
        assertThat(secondTags.getLong(1)).isEqualTo(3L);
        assertThat(secondTags.getLong(2)).isEqualTo(4L);
        assertThat(secondTags.getLong(3)).isEqualTo(5L);
        assertThat(secondTags.isNullAt(4)).isTrue();

        assertThat(converter.buildFieldMeta("tags"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Arrays.asList(0, 2),
                                        1, Arrays.asList(0, 1),
                                        2, Collections.singletonList(1)),
                                new TreeSet<Integer>(),
                                3,
                                3));
    }

    @Test
    void testBasicMapWithNullValue() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("metrics", 2));

        InternalRow row = converter.convert(GenericRow.of(stringKeyMap("a", null, "b", 20L)));
        InternalRow metrics = row.getRow(0, 4);

        // Target physical row:
        // metrics = [__field_mapping=[a, b], __col_0=null, __col_1=20,
        //            __overflow=null]
        assertThat(metrics.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(metrics.isNullAt(1)).isTrue();
        assertThat(metrics.getLong(2)).isEqualTo(20L);
        assertThat(metrics.isNullAt(3)).isTrue();
        assertThat(converter.buildFieldMeta("metrics"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1)),
                                new TreeSet<Integer>(),
                                2,
                                2));
    }

    @Test
    void testOverflowWhenExceedK() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("metrics", 2));

        InternalRow row =
                converter.convert(GenericRow.of(stringKeyMap("a", 10L, "b", 20L, "c", 30L)));
        InternalRow metrics = row.getRow(0, 4);

        // Target physical row:
        // metrics = [__field_mapping=[a, b], __col_0=10, __col_1=20,
        //            __overflow={c:30}]
        assertThat(metrics.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(metrics.getLong(1)).isEqualTo(10L);
        assertThat(metrics.getLong(2)).isEqualTo(20L);
        assertThat(metrics.getMap(3)).isEqualTo(intKeyMap(2, 30L));
        assertThat(converter.buildFieldMeta("metrics"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1)),
                                new TreeSet<>(Collections.singletonList(2)),
                                2,
                                3));
    }

    @Test
    void testEmptyAndNullMaps() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "metrics",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("metrics", 2));

        InternalRow nullRow = converter.convert(GenericRow.of((InternalMap) null));
        assertThat(nullRow.isNullAt(0)).isTrue();

        InternalRow emptyRow = converter.convert(GenericRow.of(stringKeyMap()));
        InternalRow metrics = emptyRow.getRow(0, 4);

        // Target physical row for an empty map:
        // metrics = [__field_mapping=[empty, empty], __col_0=null, __col_1=null,
        //            __overflow=null]
        assertThat(metrics.getArray(0).toIntArray()).containsExactly(-1, -1);
        assertThat(metrics.isNullAt(1)).isTrue();
        assertThat(metrics.isNullAt(2)).isTrue();
        assertThat(metrics.isNullAt(3)).isTrue();

        assertThat(converter.buildFieldMeta("metrics"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                new TreeMap<String, Integer>(),
                                new TreeMap<Integer, List<Integer>>(),
                                new TreeSet<Integer>(),
                                2,
                                0));
    }

    @Test
    void testNestedValueStruct() {
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "x", DataTypes.INT()),
                        DataTypes.FIELD(1, "y", DataTypes.DOUBLE()));
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "tags", DataTypes.MAP(DataTypes.STRING(), valueType)));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("tags", 2));

        InternalRow row =
                converter.convert(
                        GenericRow.of(
                                stringKeyMap(
                                        "a", GenericRow.of(1, 1.1D),
                                        "b", GenericRow.of(2, 2.2D),
                                        "c", GenericRow.of(3, 3.3D))));
        InternalRow tags = row.getRow(0, 4);

        // Target physical row:
        // tags = [__field_mapping=[a, b], __col_0={x=1,y=1.1}, __col_1={x=2,y=2.2},
        //         __overflow={c:{x=3,y=3.3}}]
        assertThat(tags.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(tags.getRow(1, 2)).isEqualTo(GenericRow.of(1, 1.1D));
        assertThat(tags.getRow(2, 2)).isEqualTo(GenericRow.of(2, 2.2D));
        assertThat(tags.getMap(3)).isEqualTo(intKeyMap(2, GenericRow.of(3, 3.3D)));
        assertThat(converter.buildFieldMeta("tags"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1)),
                                new TreeSet<>(Collections.singletonList(2)),
                                2,
                                3));
    }

    @Test
    void testNestedValueList() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "tags",
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("tags", 2));

        InternalRow first =
                converter.convert(
                        GenericRow.of(1, stringKeyMap("a", array(1, null, 2), "b", array(3))));
        InternalRow firstTags = first.getRow(1, 4);

        // Target physical row:
        // id = 1
        // tags = [__field_mapping=[a, b], __col_0=[1,null,2], __col_1=[3],
        //         __overflow=null]
        assertThat(firstTags.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(firstTags.getArray(1)).isEqualTo(array(1, null, 2));
        assertThat(firstTags.getArray(2)).isEqualTo(array(3));
        assertThat(firstTags.isNullAt(3)).isTrue();

        InternalRow second =
                converter.convert(GenericRow.of(2, stringKeyMap("a", array((Object) null))));
        InternalRow secondTags = second.getRow(1, 4);

        // Target physical row:
        // id = 2
        // tags = [__field_mapping=[a, empty], __col_0=[null], __col_1=null,
        //         __overflow=null]
        assertThat(secondTags.getArray(0).toIntArray()).containsExactly(0, -1);
        assertThat(secondTags.getArray(1)).isEqualTo(array((Object) null));
        assertThat(secondTags.isNullAt(2)).isTrue();
        assertThat(secondTags.isNullAt(3)).isTrue();

        InternalRow third = converter.convert(GenericRow.of(3, stringKeyMap("c", array(5, 6, 7))));
        InternalRow thirdTags = third.getRow(1, 4);

        // Target physical row:
        // id = 3
        // tags = [__field_mapping=[c, empty], __col_0=[5,6,7], __col_1=null,
        //         __overflow=null]
        assertThat(thirdTags.getArray(0).toIntArray()).containsExactly(2, -1);
        assertThat(thirdTags.getArray(1)).isEqualTo(array(5, 6, 7));
        assertThat(thirdTags.isNullAt(2)).isTrue();
        assertThat(thirdTags.isNullAt(3)).isTrue();

        InternalRow fourth =
                converter.convert(
                        GenericRow.of(
                                4,
                                stringKeyMap(
                                        "b", array(8),
                                        "a", array(9, 10),
                                        "c", array((Object) null))));
        InternalRow fourthTags = fourth.getRow(1, 4);

        // Target physical row:
        // id = 4
        // tags = [__field_mapping=[b, a], __col_0=[8], __col_1=[9,10],
        //         __overflow={c:[null]}]
        assertThat(fourthTags.getArray(0).toIntArray()).containsExactly(1, 0);
        assertThat(fourthTags.getArray(1)).isEqualTo(array(8));
        assertThat(fourthTags.getArray(2)).isEqualTo(array(9, 10));
        assertThat(fourthTags.getMap(3)).isEqualTo(intKeyMap(2, array((Object) null)));

        assertThat(converter.buildFieldMeta("tags"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Arrays.asList(0, 1),
                                        1, Arrays.asList(0, 1),
                                        2, Collections.singletonList(0)),
                                new TreeSet<>(Collections.singletonList(2)),
                                2,
                                3));
    }

    @Test
    void testNestedValueMap() {
        MapType innerMapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "nested", DataTypes.MAP(DataTypes.STRING(), innerMapType)));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("nested", 2));

        InternalRow first =
                converter.convert(
                        GenericRow.of(
                                1,
                                stringKeyMap(
                                        "a",
                                        stringKeyMap("x", 1, "y", null),
                                        "b",
                                        stringKeyMap("z", 3))));
        InternalRow firstNested = first.getRow(1, 4);

        // Target physical row:
        // id = 1
        // nested = [__field_mapping=[a, b], __col_0={x:1,y:null}, __col_1={z:3},
        //           __overflow=null]
        assertThat(firstNested.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(firstNested.getMap(1)).isEqualTo(stringKeyMap("x", 1, "y", null));
        assertThat(firstNested.getMap(2)).isEqualTo(stringKeyMap("z", 3));
        assertThat(firstNested.isNullAt(3)).isTrue();

        InternalRow second =
                converter.convert(GenericRow.of(2, stringKeyMap("c", stringKeyMap("p", null))));
        InternalRow secondNested = second.getRow(1, 4);

        // Target physical row:
        // id = 2
        // nested = [__field_mapping=[c, empty], __col_0={p:null}, __col_1=null,
        //           __overflow=null]
        assertThat(secondNested.getArray(0).toIntArray()).containsExactly(2, -1);
        assertThat(secondNested.getMap(1)).isEqualTo(stringKeyMap("p", null));
        assertThat(secondNested.isNullAt(2)).isTrue();
        assertThat(secondNested.isNullAt(3)).isTrue();

        InternalRow nullRow = converter.convert(GenericRow.of(3, null));
        assertThat(nullRow.isNullAt(1)).isTrue();

        InternalRow fourth =
                converter.convert(
                        GenericRow.of(
                                4,
                                stringKeyMap(
                                        "a",
                                        stringKeyMap("m", 7),
                                        "b",
                                        stringKeyMap("n", 8),
                                        "c",
                                        stringKeyMap("o", 9))));
        InternalRow fourthNested = fourth.getRow(1, 4);

        // Target physical row:
        // id = 4
        // nested = [__field_mapping=[a, b], __col_0={m:7}, __col_1={n:8},
        //           __overflow={c:{o:9}}]
        assertThat(fourthNested.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(fourthNested.getMap(1)).isEqualTo(stringKeyMap("m", 7));
        assertThat(fourthNested.getMap(2)).isEqualTo(stringKeyMap("n", 8));
        assertThat(fourthNested.getMap(3)).isEqualTo(intKeyMap(2, stringKeyMap("o", 9)));

        assertThat(converter.buildFieldMeta("nested"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1),
                                        2, Collections.singletonList(0)),
                                new TreeSet<>(Collections.singletonList(2)),
                                2,
                                3));
    }

    @Test
    void testNestedComplex() {
        RowType valueType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "score", DataTypes.INT()),
                        DataTypes.FIELD(1, "tags", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD(
                                2, "meta", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "data", DataTypes.MAP(DataTypes.STRING(), valueType)));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("data", 2));

        InternalRow first =
                converter.convert(
                        GenericRow.of(
                                1,
                                stringKeyMap(
                                        "a",
                                        GenericRow.of(
                                                10, stringArray("t1", "t2"), stringKeyMap("x", 1)),
                                        "b",
                                        GenericRow.of(
                                                20,
                                                stringArray("t3"),
                                                stringKeyMap("y", 2, "z", 3)))));
        InternalRow firstData = first.getRow(1, 4);

        // Target physical row:
        // id = 1
        // data = [__field_mapping=[a, b],
        //         __col_0={score=10,tags=[t1,t2],meta={x:1}},
        //         __col_1={score=20,tags=[t3],meta={y:2,z:3}}, __overflow=null]
        assertThat(firstData.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(firstData.getRow(1, 3))
                .isEqualTo(GenericRow.of(10, stringArray("t1", "t2"), stringKeyMap("x", 1)));
        assertThat(firstData.getRow(2, 3))
                .isEqualTo(GenericRow.of(20, stringArray("t3"), stringKeyMap("y", 2, "z", 3)));
        assertThat(firstData.isNullAt(3)).isTrue();

        InternalRow second =
                converter.convert(
                        GenericRow.of(
                                2,
                                stringKeyMap(
                                        "c", GenericRow.of(null, null, stringKeyMap("p", null)))));
        InternalRow secondData = second.getRow(1, 4);

        // Target physical row:
        // id = 2
        // data = [__field_mapping=[c, empty],
        //         __col_0={score=null,tags=null,meta={p:null}}, __col_1=null,
        //         __overflow=null]
        assertThat(secondData.getArray(0).toIntArray()).containsExactly(2, -1);
        assertThat(secondData.getRow(1, 3))
                .isEqualTo(GenericRow.of(null, null, stringKeyMap("p", null)));
        assertThat(secondData.isNullAt(2)).isTrue();
        assertThat(secondData.isNullAt(3)).isTrue();

        InternalRow third =
                converter.convert(
                        GenericRow.of(
                                3,
                                stringKeyMap(
                                        "a",
                                        GenericRow.of(30, stringArray(null, "t4"), stringKeyMap()),
                                        "b",
                                        GenericRow.of(null, stringArray(), stringKeyMap("q", 5)),
                                        "c",
                                        GenericRow.of(
                                                40, stringArray("t5"), stringKeyMap("r", 6)))));
        InternalRow thirdData = third.getRow(1, 4);

        // Target physical row:
        // id = 3
        // data = [__field_mapping=[a, b],
        //         __col_0={score=30,tags=[null,t4],meta={}},
        //         __col_1={score=null,tags=[],meta={q:5}},
        //         __overflow={c:{score=40,tags=[t5],meta={r:6}}}]
        assertThat(thirdData.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(thirdData.getRow(1, 3))
                .isEqualTo(GenericRow.of(30, stringArray(null, "t4"), stringKeyMap()));
        assertThat(thirdData.getRow(2, 3))
                .isEqualTo(GenericRow.of(null, stringArray(), stringKeyMap("q", 5)));
        assertThat(thirdData.getMap(3))
                .isEqualTo(
                        intKeyMap(2, GenericRow.of(40, stringArray("t5"), stringKeyMap("r", 6))));

        InternalRow nullRow = converter.convert(GenericRow.of(4, null));
        assertThat(nullRow.isNullAt(1)).isTrue();

        assertThat(converter.buildFieldMeta("data"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1),
                                        2, Collections.singletonList(0)),
                                new TreeSet<>(Collections.singletonList(2)),
                                2,
                                3));
    }

    @Test
    void testMultipleMapFields() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        DataTypes.FIELD(
                                2, "attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())));
        Map<String, Integer> fieldToNumColumns = new HashMap<>();
        fieldToNumColumns.put("tags", 2);
        fieldToNumColumns.put("attrs", 3);
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, fieldToNumColumns);

        InternalRow first =
                converter.convert(
                        GenericRow.of(
                                1,
                                stringKeyMap("a", 10L, "b", 20L),
                                stringKeyMap("x", 1.1D, "y", 2.2D)));
        InternalRow firstTags = first.getRow(1, 4);
        InternalRow firstAttrs = first.getRow(2, 5);

        // Target physical row:
        // id = 1
        // tags = [__field_mapping=[a, b], __col_0=10, __col_1=20, __overflow=null]
        // attrs = [__field_mapping=[x, y, empty], __col_0=1.1, __col_1=2.2,
        //          __col_2=null, __overflow=null]
        assertThat(firstTags.getArray(0).toIntArray()).containsExactly(0, 1);
        assertThat(firstTags.getLong(1)).isEqualTo(10L);
        assertThat(firstTags.getLong(2)).isEqualTo(20L);
        assertThat(firstTags.isNullAt(3)).isTrue();
        assertThat(firstAttrs.getArray(0).toIntArray()).containsExactly(0, 1, -1);
        assertThat(firstAttrs.getDouble(1)).isEqualTo(1.1D);
        assertThat(firstAttrs.getDouble(2)).isEqualTo(2.2D);
        assertThat(firstAttrs.isNullAt(3)).isTrue();
        assertThat(firstAttrs.isNullAt(4)).isTrue();

        InternalRow second =
                converter.convert(
                        GenericRow.of(
                                2,
                                stringKeyMap("c", 30L, "a", 40L, "b", 50L),
                                stringKeyMap("z", 3.3D)));
        InternalRow secondTags = second.getRow(1, 4);
        InternalRow secondAttrs = second.getRow(2, 5);

        // Target physical row:
        // id = 2
        // tags = [__field_mapping=[c, a], __col_0=30, __col_1=40, __overflow={b:50}]
        // attrs = [__field_mapping=[z, empty, empty], __col_0=3.3, __col_1=null,
        //          __col_2=null, __overflow=null]
        assertThat(secondTags.getArray(0).toIntArray()).containsExactly(2, 0);
        assertThat(secondTags.getLong(1)).isEqualTo(30L);
        assertThat(secondTags.getLong(2)).isEqualTo(40L);
        assertThat(secondTags.getMap(3)).isEqualTo(intKeyMap(1, 50L));
        assertThat(secondAttrs.getArray(0).toIntArray()).containsExactly(2, -1, -1);
        assertThat(secondAttrs.getDouble(1)).isEqualTo(3.3D);
        assertThat(secondAttrs.isNullAt(2)).isTrue();
        assertThat(secondAttrs.isNullAt(3)).isTrue();
        assertThat(secondAttrs.isNullAt(4)).isTrue();

        InternalRow third =
                converter.convert(
                        GenericRow.of(
                                3, null, stringKeyMap("x", 4.4D, "y", 5.5D, "z", 6.6D, "w", 7.7D)));
        InternalRow thirdAttrs = third.getRow(2, 5);

        // Target physical row:
        // id = 3
        // tags = null
        // attrs = [__field_mapping=[x, y, z], __col_0=4.4, __col_1=5.5, __col_2=6.6,
        //          __overflow={w:7.7}]
        assertThat(third.isNullAt(1)).isTrue();
        assertThat(thirdAttrs.getArray(0).toIntArray()).containsExactly(0, 1, 2);
        assertThat(thirdAttrs.getDouble(1)).isEqualTo(4.4D);
        assertThat(thirdAttrs.getDouble(2)).isEqualTo(5.5D);
        assertThat(thirdAttrs.getDouble(3)).isEqualTo(6.6D);
        assertThat(thirdAttrs.getMap(4)).isEqualTo(intKeyMap(3, 7.7D));

        assertThat(converter.shreddingFieldNames()).containsExactly("tags", "attrs");
        assertThat(converter.buildFieldMeta("tags"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("a", 0, "b", 1, "c", 2),
                                fieldToColumns(
                                        0, Arrays.asList(0, 1),
                                        1, Collections.singletonList(1),
                                        2, Collections.singletonList(0)),
                                new TreeSet<>(Collections.singletonList(1)),
                                2,
                                3));
        assertThat(converter.buildFieldMeta("attrs"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                nameToId("w", 3, "x", 0, "y", 1, "z", 2),
                                fieldToColumns(
                                        0, Collections.singletonList(0),
                                        1, Collections.singletonList(1),
                                        2, Arrays.asList(0, 2)),
                                new TreeSet<>(Collections.singletonList(3)),
                                3,
                                4));
    }

    @Test
    void testBuildFieldMetaInvalidFieldName() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(
                                1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, columns("tags", 3));

        assertThat(converter.buildFieldMeta("tags"))
                .isEqualTo(
                        new MapSharedShreddingFieldMeta(
                                new TreeMap<String, Integer>(),
                                new TreeMap<Integer, List<Integer>>(),
                                new TreeSet<Integer>(),
                                3,
                                0));
        assertThatThrownBy(() -> converter.buildFieldMeta("id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id");
        assertThatThrownBy(() -> converter.buildFieldMeta("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonexistent");
    }

    private static Map<String, Integer> columns(String fieldName, int numColumns) {
        Map<String, Integer> columns = new HashMap<>();
        columns.put(fieldName, numColumns);
        return columns;
    }

    private static GenericMap stringKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(values);
    }

    private static GenericArray array(Object... values) {
        return new GenericArray(values);
    }

    private static GenericArray stringArray(String... values) {
        Object[] internalValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            internalValues[i] = values[i] == null ? null : BinaryString.fromString(values[i]);
        }
        return new GenericArray(internalValues);
    }

    private static GenericMap intKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        return new GenericMap(values);
    }

    private static Map<String, Integer> nameToId(Object... keyValues) {
        Map<String, Integer> nameToId = new TreeMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            nameToId.put((String) keyValues[i], (Integer) keyValues[i + 1]);
        }
        return nameToId;
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, List<Integer>> fieldToColumns(Object... keyValues) {
        Map<Integer, List<Integer>> fieldToColumns = new TreeMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            fieldToColumns.put((Integer) keyValues[i], (List<Integer>) keyValues[i + 1]);
        }
        return fieldToColumns;
    }
}
