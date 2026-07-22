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

package org.apache.paimon.table.format;

import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CatalogFormatTablePartitionManager}: paging, prefix pushdown, per-request
 * batching and catalog lifecycle are transport details this adapter owns, and none of them are
 * visible through {@link FormatTablePartitionManager}.
 */
class CatalogFormatTablePartitionManagerTest {

    private static final Identifier IDENTIFIER =
            Identifier.create("catalog_partition_db", "catalog_partition_table");

    private static final List<String> PARTITION_KEYS = Arrays.asList("year", "month");

    private static final int REQUEST_SIZE = 1000;

    /** Any non-null partition predicate; the manager passes it through untouched. */
    private static final Predicate FILTER =
            new PredicateBuilder(
                            RowType.of(
                                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                                    new String[] {"year", "month"}))
                    .greaterThan(1, BinaryString.fromString("01"));

    /** Handed to the serializable loader, which cannot capture a test instance field. */
    private static Catalog staticCatalog;

    // ------------------------------------------------------------------------
    //  paging
    // ------------------------------------------------------------------------

    @Test
    void testListPartitionsFollowsEveryPage() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition first = partition("2025", "01");
        Partition second = partition("2025", "02");
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(first), "page-2"),
                        new PagedList<>(Collections.singletonList(second), null));

        List<Partition> partitions =
                partitionManager(catalog).listPartitions(Collections.emptyMap(), null);

        assertThat(partitions).containsExactly(first, second);
        ArgumentCaptor<String> pageTokens = ArgumentCaptor.forClass(String.class);
        verify(catalog, times(2))
                .listPartitionsPaged(eq(IDENTIFIER), eq(REQUEST_SIZE), pageTokens.capture(), any());
        assertThat(pageTokens.getAllValues()).containsExactly(null, "page-2");
    }

    @Test
    void testEmptyNextPageTokenEndsPaging() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition only = partition("2025", "01");
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.singletonList(only), ""));

        List<Partition> partitions =
                partitionManager(catalog).listPartitions(Collections.emptyMap(), null);

        assertThat(partitions).containsExactly(only);
        verify(catalog, times(1)).listPartitionsPaged(any(), any(), any(), any());
    }

    @Test
    void testRepeatedPageTokenFailsFast() throws Exception {
        Catalog catalog = mock(Catalog.class);
        // a -> b -> a: following the cycle would list the same pages forever.
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(partition("2025", "01")), "a"),
                        new PagedList<>(Collections.singletonList(partition("2025", "02")), "b"),
                        new PagedList<>(Collections.singletonList(partition("2025", "03")), "a"))
                .thenThrow(new AssertionError("unexpected extra page request"));

        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        assertThatThrownBy(() -> partitionManager.listPartitions(Collections.emptyMap(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeated partition page token")
                .hasMessageContaining("catalog_partition_db.catalog_partition_table");
    }

    // ------------------------------------------------------------------------
    //  filtered listing
    // ------------------------------------------------------------------------

    @Test
    void testFilteredListingPassesRequestAndPreservesSupersetAcrossPages() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition first = partition("2025", "01");
        Partition second = partition("2025", "02");
        when(catalog.listPartitionsByFilterPaged(any(), any(), any(), any(), any()))
                .thenReturn(
                        new PagedList<>(Arrays.asList(partition("2024", "12"), first), "page-2"),
                        new PagedList<>(Collections.singletonList(second), null));

        List<Partition> partitions =
                partitionManager(catalog)
                        .listPartitions(Collections.singletonMap("year", "2025"), FILTER);

        assertThat(partitions).containsExactly(first, second);
        ArgumentCaptor<String> pageTokens = ArgumentCaptor.forClass(String.class);
        verify(catalog, times(2))
                .listPartitionsByFilterPaged(
                        eq(IDENTIFIER),
                        eq(FILTER),
                        eq(REQUEST_SIZE),
                        pageTokens.capture(),
                        eq("year=2025/%"));
        assertThat(pageTokens.getAllValues()).containsExactly(null, "page-2");
        verify(catalog, never()).listPartitionsPaged(any(), any(), any(), any());
    }

    @Test
    void testFilteredListingFollowsSparsePages() throws Exception {
        // Filtering may leave a page empty while more partitions remain to check: only an empty
        // token ends the loop.
        Catalog catalog = mock(Catalog.class);
        Partition only = partition("2025", "02");
        when(catalog.listPartitionsByFilterPaged(any(), any(), any(), any(), any()))
                .thenReturn(
                        new PagedList<>(Collections.emptyList(), "page-2"),
                        new PagedList<>(Collections.singletonList(only), null));

        List<Partition> partitions =
                partitionManager(catalog).listPartitions(Collections.emptyMap(), FILTER);

        assertThat(partitions).containsExactly(only);
        verify(catalog, times(2)).listPartitionsByFilterPaged(any(), any(), any(), any(), any());
    }

    @Test
    void testCatalogDefaultFilteredListingDelegatesToPlainListing() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition only = partition("2025", "01");
        when(catalog.listPartitionsByFilterPaged(any(), any(), any(), any(), any()))
                .thenCallRealMethod();
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.singletonList(only), null));

        PagedList<Partition> page =
                catalog.listPartitionsByFilterPaged(
                        IDENTIFIER, FILTER, REQUEST_SIZE, "page-2", "year=2025/%");

        assertThat(page.getElements()).containsExactly(only);
        verify(catalog)
                .listPartitionsPaged(
                        eq(IDENTIFIER), eq(REQUEST_SIZE), eq("page-2"), eq("year=2025/%"));
    }

    @Test
    void testOtherFilteredFailuresDoNotFallBack() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Catalog.TableNoPermissionException failure =
                new Catalog.TableNoPermissionException(IDENTIFIER);
        when(catalog.listPartitionsByFilterPaged(any(), any(), any(), any(), any()))
                .thenThrow(failure);
        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        Throwable thrown =
                catchThrowable(
                        () -> partitionManager.listPartitions(Collections.emptyMap(), FILTER));

        assertThat(thrown).isSameAs(failure);
        verify(catalog, never()).listPartitionsPaged(any(), any(), any(), any());
    }

    @Test
    void testMissingTableDoesNotTriggerPlainListing() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Catalog.TableNotExistException notExist = new Catalog.TableNotExistException(IDENTIFIER);
        when(catalog.listPartitionsByFilterPaged(any(), any(), any(), any(), any()))
                .thenThrow(notExist);
        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        Throwable thrown =
                catchThrowable(
                        () -> partitionManager.listPartitions(Collections.emptyMap(), FILTER));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("list partitions")
                .hasMessageContaining("catalog_partition_db.catalog_partition_table");
        assertThat(thrown.getCause()).isSameAs(notExist);
        verify(catalog, never()).listPartitionsPaged(any(), any(), any(), any());
    }

    // ------------------------------------------------------------------------
    //  prefix pushdown and client side filtering
    // ------------------------------------------------------------------------

    @Test
    void testLeadingPrefixIsPushedDownAsPattern() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.emptyList(), null));

        partitionManager(catalog).listPartitions(Collections.singletonMap("year", "2025"), null);

        verify(catalog)
                .listPartitionsPaged(eq(IDENTIFIER), eq(REQUEST_SIZE), isNull(), eq("year=2025/%"));
    }

    @Test
    void testEmptyPrefixPushesDownNoPattern() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.emptyList(), null));

        partitionManager(catalog).listPartitions(Collections.emptyMap(), null);

        verify(catalog).listPartitionsPaged(eq(IDENTIFIER), eq(REQUEST_SIZE), isNull(), isNull());
    }

    @Test
    void testPrefixIsEnforcedOnCatalogsThatIgnoreThePattern() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition matching = partition("2025", "01");
        Partition other = partition("2024", "01");
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Arrays.asList(matching, other), null));

        List<Partition> partitions =
                partitionManager(catalog)
                        .listPartitions(Collections.singletonMap("year", "2025"), null);

        assertThat(partitions).containsExactly(matching);
    }

    @Test
    void testNonLeadingPrefixIsRejected() {
        Catalog catalog = mock(Catalog.class);
        FormatTablePartitionManager partitionManager = partitionManager(catalog);
        // "month" without "year" cannot be expressed as a partition path prefix.
        Map<String, String> prefix = Collections.singletonMap("month", "10");

        assertThatThrownBy(() -> partitionManager.listPartitions(prefix, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leading prefix");
    }

    // ------------------------------------------------------------------------
    //  batching
    // ------------------------------------------------------------------------

    @Test
    void testIdempotentCreateIsSplitIntoRequests() throws Exception {
        Catalog catalog = mock(Catalog.class);
        List<Map<String, String>> specs = specs(2500);

        partitionManager(catalog).createPartitions(specs, true);

        List<List<Map<String, String>>> batches = capturedCreates(catalog, true, 3);
        assertThat(batches).extracting(List::size).containsExactly(1000, 1000, 500);
        assertThat(flatten(batches)).isEqualTo(specs);
    }

    @Test
    void testStrictCreateStaysOneRequest() throws Exception {
        Catalog catalog = mock(Catalog.class);
        List<Map<String, String>> specs = specs(2500);

        partitionManager(catalog).createPartitions(specs, false);

        // Rejecting the whole batch when any partition exists only holds if it is never split.
        List<List<Map<String, String>>> batches = capturedCreates(catalog, false, 1);
        assertThat(batches.get(0)).isEqualTo(specs);
    }

    @Test
    void testDropIsSplitIntoRequests() throws Exception {
        Catalog catalog = mock(Catalog.class);
        List<Map<String, String>> specs = specs(2500);

        partitionManager(catalog).dropPartitions(specs);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Map<String, String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(catalog, times(3)).dropPartitions(eq(IDENTIFIER), captor.capture());
        assertThat(captor.getAllValues()).extracting(List::size).containsExactly(1000, 1000, 500);
        assertThat(flatten(captor.getAllValues())).isEqualTo(specs);
    }

    @Test
    void testListByNamesIsSplitAndUnioned() throws Exception {
        Catalog catalog = mock(Catalog.class);
        List<Map<String, String>> specs = specs(2500);
        Partition first = partition("2025", "0000");
        Partition second = partition("2025", "1000");
        Partition third = partition("2025", "2000");
        when(catalog.listPartitionsByNames(any(), anyList()))
                .thenReturn(
                        Collections.singletonList(first),
                        Collections.singletonList(second),
                        Collections.singletonList(third));

        List<Partition> found = partitionManager(catalog).listPartitionsByNames(specs);

        assertThat(found).containsExactly(first, second, third);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Map<String, String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(catalog, times(3)).listPartitionsByNames(eq(IDENTIFIER), captor.capture());
        assertThat(captor.getAllValues()).extracting(List::size).containsExactly(1000, 1000, 500);
        assertThat(flatten(captor.getAllValues())).isEqualTo(specs);
    }

    @Test
    void testEmptyInputTouchesNoCatalog() {
        Catalog createCatalog = mock(Catalog.class);
        partitionManager(createCatalog).createPartitions(Collections.emptyList(), true);
        verifyNoInteractions(createCatalog);

        Catalog dropCatalog = mock(Catalog.class);
        partitionManager(dropCatalog).dropPartitions(Collections.emptyList());
        verifyNoInteractions(dropCatalog);

        Catalog listCatalog = mock(Catalog.class);
        assertThat(partitionManager(listCatalog).listPartitionsByNames(Collections.emptyList()))
                .isEmpty();
        verifyNoInteractions(listCatalog);
    }

    // ------------------------------------------------------------------------
    //  catalog lifecycle
    // ------------------------------------------------------------------------

    @Test
    void testEveryOperationClosesTheCatalog() throws Exception {
        Catalog listCatalog = mock(Catalog.class);
        when(listCatalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.emptyList(), null));
        partitionManager(listCatalog).listPartitions(Collections.emptyMap(), null);
        verify(listCatalog).close();

        Catalog byNamesCatalog = mock(Catalog.class);
        when(byNamesCatalog.listPartitionsByNames(any(), anyList()))
                .thenReturn(Collections.emptyList());
        partitionManager(byNamesCatalog).listPartitionsByNames(specs(1));
        verify(byNamesCatalog).close();

        Catalog createCatalog = mock(Catalog.class);
        partitionManager(createCatalog).createPartitions(specs(1), true);
        verify(createCatalog).close();

        Catalog dropCatalog = mock(Catalog.class);
        partitionManager(dropCatalog).dropPartitions(specs(1));
        verify(dropCatalog).close();
    }

    @Test
    void testFailingOperationStillClosesTheCatalog() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("catalog unavailable"));
        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        assertThatThrownBy(() -> partitionManager.listPartitions(Collections.emptyMap(), null))
                .isInstanceOf(RuntimeException.class);

        verify(catalog).close();
    }

    @Test
    void testRejectedPrefixNeverLoadsACatalog() {
        Catalog catalog = mock(Catalog.class);
        FormatTablePartitionManager partitionManager = partitionManager(catalog);
        Map<String, String> prefix = Collections.singletonMap("month", "10");

        assertThatThrownBy(() -> partitionManager.listPartitions(prefix, null))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(catalog);
    }

    // ------------------------------------------------------------------------
    //  error translation
    // ------------------------------------------------------------------------

    @Test
    void testCheckedExceptionIsWrappedWithTableContext() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Catalog.TableNotExistException notExist = new Catalog.TableNotExistException(IDENTIFIER);
        when(catalog.listPartitionsPaged(any(), any(), any(), any())).thenThrow(notExist);
        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        Throwable thrown =
                catchThrowable(() -> partitionManager.listPartitions(Collections.emptyMap(), null));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("list partitions")
                .hasMessageContaining("catalog_partition_db.catalog_partition_table");
        assertThat(thrown.getCause()).isSameAs(notExist);
    }

    @Test
    void testRuntimeExceptionIsRethrownUnchanged() throws Exception {
        Catalog catalog = mock(Catalog.class);
        RuntimeException failure = new IllegalStateException("catalog unavailable");
        doThrow(failure).when(catalog).createPartitions(any(), anyList(), anyBoolean());
        FormatTablePartitionManager partitionManager = partitionManager(catalog);

        Throwable thrown = catchThrowable(() -> partitionManager.createPartitions(specs(1), true));

        assertThat(thrown).isSameAs(failure);
    }

    // ------------------------------------------------------------------------
    //  serializability
    // ------------------------------------------------------------------------

    @Test
    void testSurvivesJavaSerialization() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition only = partition("2025", "01");
        when(catalog.listPartitionsPaged(any(), any(), any(), any()))
                .thenReturn(new PagedList<>(Collections.singletonList(only), null));
        staticCatalog = catalog;

        FormatTablePartitionManager partitionManager =
                FormatTablePartitionManager.create(IDENTIFIER, PARTITION_KEYS, new TestLoader());
        FormatTablePartitionManager cloned = InstantiationUtil.clone(partitionManager);

        assertThat(cloned.listPartitions(Collections.emptyMap(), null)).containsExactly(only);
        verify(catalog).close();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private static FormatTablePartitionManager partitionManager(Catalog catalog) {
        return FormatTablePartitionManager.create(IDENTIFIER, PARTITION_KEYS, () -> catalog);
    }

    private static List<List<Map<String, String>>> capturedCreates(
            Catalog catalog, boolean ignoreIfExists, int expectedRequests) throws Exception {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Map<String, String>>> captor = ArgumentCaptor.forClass(List.class);
        verify(catalog, times(expectedRequests))
                .createPartitions(eq(IDENTIFIER), captor.capture(), eq(ignoreIfExists));
        return captor.getAllValues();
    }

    private static List<Map<String, String>> flatten(List<List<Map<String, String>>> batches) {
        return batches.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private static Partition partition(String year, String month) {
        return new Partition(spec(year, month), 0, 0, 0, 0, -1, false);
    }

    private static Map<String, String> spec(String year, String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return spec;
    }

    private static List<Map<String, String>> specs(int count) {
        List<Map<String, String>> specs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            specs.add(spec("2025", String.format("%04d", i)));
        }
        return specs;
    }

    /** A {@link CatalogLoader} that survives serialization by holding no state of its own. */
    private static class TestLoader implements CatalogLoader {

        private static final long serialVersionUID = 1L;

        @Override
        public Catalog load() {
            return staticCatalog;
        }
    }
}
