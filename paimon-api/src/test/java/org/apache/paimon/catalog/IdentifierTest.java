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

package org.apache.paimon.catalog;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link Identifier}. */
public class IdentifierTest {

    @Test
    public void testFromStringBasic() {
        Identifier identifier = Identifier.fromString("database.table");
        assertEquals("database", identifier.getDatabaseName());
        assertEquals("table", identifier.getObjectName());
    }

    @Test
    public void testFromStringWithDotsInBackticks() {
        Identifier identifier1 = Identifier.fromString("`database.with.dots`.table");
        assertEquals("`database.with.dots`", identifier1.getDatabaseName());
        assertEquals("table", identifier1.getObjectName());

        Identifier identifier2 = Identifier.fromString("database.`table.with.dots`");
        assertEquals("database", identifier2.getDatabaseName());
        assertEquals("`table.with.dots`", identifier2.getObjectName());

        Identifier identifier3 = Identifier.fromString("`database.with.dots`.`table.with.dots`");
        assertEquals("`database.with.dots`", identifier3.getDatabaseName());
        assertEquals("`table.with.dots`", identifier3.getObjectName());
    }

    @Test
    public void testFromStringWithNestedBackticks() {
        Identifier identifier = Identifier.fromString("`database`.`table`");
        assertEquals("`database`", identifier.getDatabaseName());
        assertEquals("`table`", identifier.getObjectName());
    }

    @Test
    public void testFromStringWithMultipleDotsInBackticks() {
        Identifier identifier1 = Identifier.fromString("`database...multiple.dots`.`table..dots`");
        assertEquals("`database...multiple.dots`", identifier1.getDatabaseName());
        assertEquals("`table..dots`", identifier1.getObjectName());

        Identifier identifier2 = Identifier.fromString("`a.b.c.d.e`.`x.y.z`");
        assertEquals("`a.b.c.d.e`", identifier2.getDatabaseName());
        assertEquals("`x.y.z`", identifier2.getObjectName());
    }

    @Test
    public void testFromStringWithMultipleDotsOutsideBackticks() {
        Identifier identifier1 = Identifier.fromString("database.function.name");
        assertEquals("database", identifier1.getDatabaseName());
        assertEquals("function.name", identifier1.getObjectName());

        Identifier identifier2 = Identifier.fromString("`database.with.dots`.function.name");
        assertEquals("`database.with.dots`", identifier2.getDatabaseName());
        assertEquals("function.name", identifier2.getObjectName());
    }

    @Test
    public void testFromStringWithUnbalancedBackticks() {
        Identifier identifier1 = Identifier.fromString("database.table`name");
        assertEquals("database", identifier1.getDatabaseName());
        assertEquals("table`name", identifier1.getObjectName());

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Identifier.fromString("`database.table");
                });
    }

    @Test
    public void testFromStringInvalidFormat() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Identifier.fromString("nodatabase");
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Identifier.fromString("");
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Identifier.fromString(null);
                });

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Identifier.fromString("   ");
                });
    }
}
