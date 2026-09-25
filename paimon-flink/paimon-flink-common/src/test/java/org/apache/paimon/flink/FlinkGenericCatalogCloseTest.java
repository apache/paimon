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

package org.apache.paimon.flink;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests that {@link FlinkGenericCatalog} releases both catalogs it wraps. */
class FlinkGenericCatalogCloseTest {

    private FlinkCatalog paimonCatalog() {
        FlinkCatalog result = mock(FlinkCatalog.class);
        when(result.getName()).thenReturn("paimon");
        when(result.getDefaultDatabase()).thenReturn("default");
        return result;
    }

    /** The two catalogs are separate resources; one failing must not strand the other. */
    @Test
    void testCloseClosesTheFlinkCatalogWhenPaimonFails() {
        FlinkCatalog paimon = paimonCatalog();
        Catalog flink = mock(Catalog.class);
        CatalogException expected = new CatalogException("paimon is down");
        doThrow(expected).when(paimon).close();

        FlinkGenericCatalog catalog = new FlinkGenericCatalog(paimon, flink);
        assertThatThrownBy(catalog::close).isSameAs(expected);

        verify(flink).close();
    }

    /** Both failing must surface the first one, with the second attached rather than dropped. */
    @Test
    void testCloseKeepsBothFailures() {
        FlinkCatalog paimon = paimonCatalog();
        Catalog flink = mock(Catalog.class);
        CatalogException paimonFailure = new CatalogException("paimon is down");
        CatalogException flinkFailure = new CatalogException("flink is down");
        doThrow(paimonFailure).when(paimon).close();
        doThrow(flinkFailure).when(flink).close();

        FlinkGenericCatalog catalog = new FlinkGenericCatalog(paimon, flink);
        assertThatThrownBy(catalog::close).isSameAs(paimonFailure);
        assertThat(paimonFailure.getSuppressed()).containsExactly(flinkFailure);
    }

    /** open() is the same pairing in reverse: a caller does not close what failed to open. */
    @Test
    void testOpenClosesThePaimonCatalogWhenFlinkFails() {
        FlinkCatalog paimon = paimonCatalog();
        Catalog flink = mock(Catalog.class);
        CatalogException expected = new CatalogException("flink is down");
        doThrow(expected).when(flink).open();

        FlinkGenericCatalog catalog = new FlinkGenericCatalog(paimon, flink);
        assertThatThrownBy(catalog::open).isSameAs(expected);

        verify(paimon).close();
    }

    /** Control: nothing fails, both are closed exactly once and nothing is thrown. */
    @Test
    void testCloseClosesBoth() {
        FlinkCatalog paimon = paimonCatalog();
        Catalog flink = mock(Catalog.class);

        new FlinkGenericCatalog(paimon, flink).close();

        verify(paimon).close();
        verify(flink).close();
    }
}
