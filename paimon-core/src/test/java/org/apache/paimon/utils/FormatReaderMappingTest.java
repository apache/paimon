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

package org.apache.paimon.utils;

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.RichLimit;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.apache.paimon.predicate.LimitDirection.HEAD;
import static org.apache.paimon.predicate.LimitDirection.TAIL;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.apache.paimon.utils.FormatReaderMapping.Builder.tryConvertTopNToLimit;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FormatReaderMapping.Builder}. */
public class FormatReaderMappingTest {

    @Test
    public void testTrimKeyFields() {
        List<DataField> keyFields = new ArrayList<>();
        List<DataField> allFields = new ArrayList<>();
        List<DataField> testFields = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            keyFields.add(
                    new DataField(
                            SpecialFields.KEY_FIELD_ID_START + i,
                            SpecialFields.KEY_FIELD_PREFIX + i,
                            DataTypes.STRING()));
        }

        allFields.addAll(keyFields);
        for (int i = 0; i < 20; i++) {
            allFields.add(new DataField(i, String.valueOf(i), DataTypes.STRING()));
        }

        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + 1,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 3,
                        SpecialFields.KEY_FIELD_PREFIX + 3,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 5,
                        SpecialFields.KEY_FIELD_PREFIX + 5,
                        DataTypes.STRING()));
        testFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 7,
                        SpecialFields.KEY_FIELD_PREFIX + 7,
                        DataTypes.STRING()));
        testFields.add(new DataField(3, String.valueOf(3), DataTypes.STRING()));
        testFields.add(new DataField(4, String.valueOf(4), DataTypes.STRING()));
        testFields.add(new DataField(5, String.valueOf(5), DataTypes.STRING()));
        testFields.add(new DataField(1, String.valueOf(1), DataTypes.STRING()));
        testFields.add(new DataField(6, String.valueOf(6), DataTypes.STRING()));

        Pair<int[], RowType> res = FormatReaderMapping.Builder.trimKeyFields(testFields, allFields);

        assertThat(res.getKey()).containsExactly(0, 1, 2, 3, 1, 4, 2, 0, 5);

        List<DataField> fields = res.getRight().getFields();
        assertThat(fields.size()).isEqualTo(6);
        assertThat(fields.get(0).id()).isEqualTo(1);
        assertThat(fields.get(1).id()).isEqualTo(3);
        assertThat(fields.get(2).id()).isEqualTo(5);
        assertThat(fields.get(3).id()).isEqualTo(7);
        assertThat(fields.get(4).id()).isEqualTo(4);
        assertThat(fields.get(5).id()).isEqualTo(6);
    }

    @Test
    public void testTrimKeyWithIndexMapping() {
        List<DataField> readTableFields = new ArrayList<>();
        List<DataField> readDataFields = new ArrayList<>();

        readTableFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + "a",
                        DataTypes.STRING()));
        readTableFields.add(new DataField(0, "0", DataTypes.STRING()));
        readTableFields.add(new DataField(1, "a", DataTypes.STRING()));
        readTableFields.add(new DataField(2, "2", DataTypes.STRING()));
        readTableFields.add(new DataField(3, "3", DataTypes.STRING()));

        readDataFields.add(
                new DataField(
                        SpecialFields.KEY_FIELD_ID_START + 1,
                        SpecialFields.KEY_FIELD_PREFIX + "a",
                        DataTypes.STRING()));
        readDataFields.add(new DataField(0, "0", DataTypes.STRING()));
        readDataFields.add(new DataField(1, "a", DataTypes.STRING()));
        readDataFields.add(new DataField(3, "3", DataTypes.STRING()));

        // build index cast mapping
        IndexCastMapping indexCastMapping =
                SchemaEvolutionUtil.createIndexCastMapping(readTableFields, readDataFields);

        // map from key fields reading to value fields reading
        Pair<int[], RowType> trimmedKeyPair =
                FormatReaderMapping.Builder.trimKeyFields(readDataFields, readDataFields);

        FormatReaderMapping formatReaderMapping =
                new FormatReaderMapping(
                        indexCastMapping.getIndexMapping(),
                        indexCastMapping.getCastMapping(),
                        trimmedKeyPair.getLeft(),
                        null,
                        null,
                        null,
                        null,
                        Collections.emptyMap(),
                        null,
                        null);

        assertThat(formatReaderMapping.getIndexMapping()).containsExactly(0, 1, 0, -1, 2);
        List<DataField> trimmed = trimmedKeyPair.getRight().getFields();
        assertThat(trimmed.get(0).id()).isEqualTo(1);
        assertThat(trimmed.get(1).id()).isEqualTo(0);
        assertThat(trimmed.get(2).id()).isEqualTo(3);
        assertThat(trimmed.size()).isEqualTo(3);
    }

    @Test
    public void testTryConvertTopNToLimitWithSinglePrimaryKey() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()));
        List<String> primaryKeys = singletonList("id");
        Map<String, String> options = new HashMap<>();
        TableSchema schema =
                new TableSchema(1, fields, 10, Collections.emptyList(), primaryKeys, options, "");
        FieldRef idRef = new FieldRef(0, "id", DataTypes.INT());
        FieldRef f1Ref = new FieldRef(1, "f1", DataTypes.INT());

        // test only pk
        TopN topN01 = new TopN(idRef, ASCENDING, NULLS_FIRST, 1);
        Optional<RichLimit> test01 = tryConvertTopNToLimit(topN01, null, schema, true);
        assertThat(test01).isPresent();
        assertThat(test01.get()).isEqualTo(new RichLimit(1, HEAD));

        TopN topN02 = new TopN(idRef, DESCENDING, NULLS_FIRST, 1);
        Optional<RichLimit> test02 = tryConvertTopNToLimit(topN02, null, schema, true);
        assertThat(test02).isPresent();
        assertThat(test02.get()).isEqualTo(new RichLimit(1, TAIL));

        // test with non-primary-key
        TopN topN03 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, DESCENDING, NULLS_FIRST),
                                new SortValue(f1Ref, ASCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test03 = tryConvertTopNToLimit(topN03, null, schema, true);
        assertThat(test03).isPresent();
        assertThat(test03.get()).isEqualTo(new RichLimit(1, TAIL));

        // test empty
        TopN topN04 = new TopN(f1Ref, DESCENDING, NULLS_FIRST, 1);
        Optional<RichLimit> test04 = tryConvertTopNToLimit(topN04, null, schema, true);
        assertThat(test04).isEmpty();
    }

    @Test
    public void testTryConvertTopNToLimitWithMultiPrimaryKey() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()));
        List<String> primaryKeys = Arrays.asList("id", "f1");
        Map<String, String> options = new HashMap<>();
        TableSchema schema =
                new TableSchema(1, fields, 10, Collections.emptyList(), primaryKeys, options, "");
        FieldRef idRef = new FieldRef(0, "id", DataTypes.INT());
        FieldRef f1Ref = new FieldRef(1, "f1", DataTypes.INT());
        FieldRef f2Ref = new FieldRef(2, "f2", DataTypes.INT());

        // test matches
        TopN topN01 = new TopN(singletonList(new SortValue(idRef, ASCENDING, NULLS_FIRST)), 1);
        Optional<RichLimit> test01 = tryConvertTopNToLimit(topN01, null, schema, true);
        assertThat(test01).isPresent();
        assertThat(test01.get()).isEqualTo(new RichLimit(1, HEAD));

        TopN topN02 = new TopN(singletonList(new SortValue(idRef, DESCENDING, NULLS_FIRST)), 1);
        Optional<RichLimit> test02 = tryConvertTopNToLimit(topN02, null, schema, true);
        assertThat(test02).isPresent();
        assertThat(test02.get()).isEqualTo(new RichLimit(1, TAIL));

        // ASCENDING
        TopN topN03 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, ASCENDING, NULLS_FIRST),
                                new SortValue(f1Ref, ASCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test03 = tryConvertTopNToLimit(topN03, null, schema, true);
        assertThat(test03).isPresent();
        assertThat(test03.get()).isEqualTo(new RichLimit(1, HEAD));

        // DESCENDING
        TopN topN04 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, DESCENDING, NULLS_FIRST),
                                new SortValue(f1Ref, DESCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test04 = tryConvertTopNToLimit(topN04, null, schema, true);
        assertThat(test04).isPresent();
        assertThat(test04.get()).isEqualTo(new RichLimit(1, TAIL));

        // with non-primary keys
        TopN topN05 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, DESCENDING, NULLS_FIRST),
                                new SortValue(f1Ref, DESCENDING, NULLS_FIRST),
                                new SortValue(f2Ref, ASCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test05 = tryConvertTopNToLimit(topN05, null, schema, true);
        assertThat(test05).isPresent();
        assertThat(test05.get()).isEqualTo(new RichLimit(1, TAIL));

        // test not matches
        // only contains a part the primary key
        TopN topN06 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, ASCENDING, NULLS_FIRST),
                                new SortValue(f2Ref, ASCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test06 = tryConvertTopNToLimit(topN06, null, schema, true);
        assertThat(test06).isEmpty();

        // position not matches
        TopN topN07 = new TopN(singletonList(new SortValue(f1Ref, ASCENDING, NULLS_FIRST)), 1);
        Optional<RichLimit> test07 = tryConvertTopNToLimit(topN07, null, schema, true);
        assertThat(test07).isEmpty();
        TopN topN08 = new TopN(singletonList(new SortValue(f2Ref, ASCENDING, NULLS_FIRST)), 1);
        Optional<RichLimit> test08 = tryConvertTopNToLimit(topN08, null, schema, true);
        assertThat(test08).isEmpty();

        // the direction not same
        TopN topN09 =
                new TopN(
                        Arrays.asList(
                                new SortValue(idRef, ASCENDING, NULLS_FIRST),
                                new SortValue(f1Ref, DESCENDING, NULLS_FIRST)),
                        1);
        Optional<RichLimit> test09 = tryConvertTopNToLimit(topN09, null, schema, true);
        assertThat(test09).isEmpty();
    }
}
