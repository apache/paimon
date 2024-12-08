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
    private Map<String, String> tableMapping;

    @Test
    public void testConvertTableName() {
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("mapped_src", "mapped_TGT");
        TableNameConverter caseConverter =
                new TableNameConverter(true, true, "pre_", "_pos", tableMapping);
        Assert.assertEquals(caseConverter.convert("mapped_SRC"), "mapped_TGT");

        Assert.assertEquals(caseConverter.convert("unmapped_src"), "pre_unmapped_src_pos");

        TableNameConverter incaseConverter =
                new TableNameConverter(false, true, "pre_", "_pos", tableMapping);
        Assert.assertEquals(incaseConverter.convert("mapped_src"), "mapped_tgt");
        Assert.assertEquals(incaseConverter.convert("unmapped_src"), "pre_unmapped_src_pos");
    }
}
