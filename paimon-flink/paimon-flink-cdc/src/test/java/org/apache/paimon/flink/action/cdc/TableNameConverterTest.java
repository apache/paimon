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

package org.apache.paimon.flink.action.cdc;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link TableNameConverter}. */
public class TableNameConverterTest {

    @Test
    public void testConvertTableName() {
        Map<String, String> tableMapping = new HashMap<>(1);
        tableMapping.put("mapped_src", "mapped_TGT");
        TableNameConverter caseConverter =
                new TableNameConverter(true, true, "pre_", "_pos", tableMapping);
        Assert.assertEquals(caseConverter.convert("", "mapped_SRC"), "mapped_TGT");

        Assert.assertEquals(caseConverter.convert("", "unmapped_src"), "pre_unmapped_src_pos");

        TableNameConverter noCaseConverter =
                new TableNameConverter(false, true, "pre_", "_pos", tableMapping);
        Assert.assertEquals(noCaseConverter.convert("", "mapped_src"), "mapped_tgt");
        Assert.assertEquals(noCaseConverter.convert("", "unmapped_src"), "pre_unmapped_src_pos");
    }

    @Test
    public void testConvertTableNameByDBPrefix_Suffix() {
        Map<String, String> dbPrefix = new HashMap<>(2);
        dbPrefix.put("db_with_prefix", "db_pref_");
        dbPrefix.put("db_with_prefix_suffix", "db_pref_");

        Map<String, String> dbSuffix = new HashMap<>(2);
        dbSuffix.put("db_with_suffix", "_db_suff");
        dbSuffix.put("db_with_prefix_suffix", "_db_suff");

        TableNameConverter tblNameConverter =
                new TableNameConverter(false, true, dbPrefix, dbSuffix, "pre_", "_suf", null);

        // Tables in the specified db should have the specified prefix and suffix.

        // db prefix + normal suffix
        Assert.assertEquals(
                "db_pref_table_name_suf", tblNameConverter.convert("db_with_prefix", "table_name"));

        // normal prefix + db suffix
        Assert.assertEquals(
                "pre_table_name_db_suff", tblNameConverter.convert("db_with_suffix", "table_name"));

        // db prefix + db suffix
        Assert.assertEquals(
                "db_pref_table_name_db_suff",
                tblNameConverter.convert("db_with_prefix_suffix", "table_name"));

        // only normal prefix and suffix
        Assert.assertEquals(
                "pre_table_name_suf",
                tblNameConverter.convert("db_without_prefix_suffix", "table_name"));
    }
}
