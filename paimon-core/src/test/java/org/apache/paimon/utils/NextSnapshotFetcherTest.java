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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.OutOfRangeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.paimon.utils.NextSnapshotFetcher.RANGE_CHECK_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link NextSnapshotFetcher}. */
@ExtendWith(MockitoExtension.class)
public class NextSnapshotFetcherTest {

    @Mock private SnapshotManager snapshotManager;
    @Mock private ChangelogManager changelogManager;
    @Mock private Snapshot mockSnapshot;
    @Mock private Changelog mockChangelog;

    private NextSnapshotFetcher fetcher;

    @Test
    public void testGetNextSnapshotFromSnapshotManager() {
        // Arrange
        long nextSnapshotId = 5;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(true);
        when(snapshotManager.snapshot(nextSnapshotId)).thenReturn(mockSnapshot);
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act
        Snapshot actualSnapshot = fetcher.getNextSnapshot(nextSnapshotId);

        // Assert
        assertThat(actualSnapshot).isSameAs(mockSnapshot);
        verify(snapshotManager).snapshot(nextSnapshotId);
        verify(changelogManager, never()).longLivedChangelogExists(anyLong());
    }

    @Test
    public void testGetNextSnapshotFromChangelogManager() {
        // Arrange
        long nextSnapshotId = 10;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(changelogManager.longLivedChangelogExists(nextSnapshotId)).thenReturn(true);
        when(changelogManager.changelog(nextSnapshotId)).thenReturn(mockChangelog);
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, true);

        // Act
        Snapshot actualSnapshot = fetcher.getNextSnapshot(nextSnapshotId);

        // Assert
        assertThat(actualSnapshot).isSameAs(mockChangelog);
        verify(changelogManager).changelog(nextSnapshotId);
    }

    @Test
    public void testGetNextSnapshotReturnsNullWhenNotFound() {
        // Arrange
        long nextSnapshotId = 3;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(changelogManager.longLivedChangelogExists(nextSnapshotId)).thenReturn(false);
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, true);

        // Act
        Snapshot actualSnapshot = fetcher.getNextSnapshot(nextSnapshotId);

        // Assert
        assertThat(actualSnapshot).isNull();
    }

    @Test
    public void testGetNextSnapshotReturnsNullWhenChangelogDecoupledIsFalse() {
        // Arrange
        long nextSnapshotId = 3;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act
        Snapshot actualSnapshot = fetcher.getNextSnapshot(nextSnapshotId);

        // Assert
        assertThat(actualSnapshot).isNull();
        verify(changelogManager, never()).longLivedChangelogExists(anyLong());
    }

    @Test
    public void testRangeCheckThrowsExceptionForExpiredSnapshot() {
        // Arrange
        long nextSnapshotId = 2;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(snapshotManager.earliestSnapshotId()).thenReturn(3L); // earliest is after next
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act: Call RANGE_CHECK_INTERVAL - 1 times, should be null and not throw
        for (int i = 0; i < RANGE_CHECK_INTERVAL - 1; i++) {
            assertThat(fetcher.getNextSnapshot(nextSnapshotId)).isNull();
        }

        // Assert: 16th call should trigger rangeCheck and throw
        assertThatThrownBy(() -> fetcher.getNextSnapshot(nextSnapshotId))
                .isInstanceOf(OutOfRangeException.class)
                .hasMessageContaining(
                        "The snapshot with id 2 has expired. You can: 1. increase the snapshot or changelog expiration time. "
                                + "2. use consumer-id to ensure that unconsumed snapshots will not be expired.");
    }

    @Test
    public void testRangeCheckDoesNotThrowForExpiredSnapshotWhenChangelogDecoupled() {
        // Arrange
        long nextSnapshotId = 2;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(changelogManager.longLivedChangelogExists(nextSnapshotId)).thenReturn(false);
        when(snapshotManager.earliestSnapshotId()).thenReturn(3L); // earliest is after next
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, true);

        // Act & Assert: Call 16 times, should always be null and not throw
        for (int i = 0; i < 16; i++) {
            assertThat(fetcher.getNextSnapshot(nextSnapshotId)).isNull();
        }
        // No exception should be thrown
    }

    @Test
    public void testRangeCheckThrowsExceptionForTooBigSnapshotId() {
        // Arrange
        long nextSnapshotId = 10;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(snapshotManager.earliestSnapshotId()).thenReturn(1L);
        when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(8L); // next > latest + 1
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act: Call RANGE_CHECK_INTERVAL - 1 times
        for (int i = 0; i < RANGE_CHECK_INTERVAL - 1; i++) {
            fetcher.getNextSnapshot(nextSnapshotId);
        }

        // Assert: 16th call throws
        assertThatThrownBy(() -> fetcher.getNextSnapshot(nextSnapshotId))
                .isInstanceOf(OutOfRangeException.class)
                .hasMessageContaining(
                        "The next expected snapshot is too big! Most possible cause might be the table had been recreated.");
    }

    @Test
    public void testRangeCheckDoesNotThrowWhenWaitingForNextSequentialSnapshot() {
        // Arrange
        long nextSnapshotId = 9;
        when(snapshotManager.snapshotExists(nextSnapshotId)).thenReturn(false);
        when(snapshotManager.earliestSnapshotId()).thenReturn(1L);
        when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(8L); // next == latest + 1
        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act & Assert: Call 16 times, should always be null and not throw
        for (int i = 0; i < 16; i++) {
            assertThat(fetcher.getNextSnapshot(nextSnapshotId)).isNull();
        }
        // No exception should be thrown
    }

    @Test
    public void testRangeCheckCounterResetsOnSuccess() {
        // Arrange
        long missingSnapshotId = 5;
        long existingSnapshotId = 6;
        when(snapshotManager.snapshotExists(missingSnapshotId)).thenReturn(false);
        when(snapshotManager.snapshotExists(existingSnapshotId)).thenReturn(true);
        when(snapshotManager.snapshot(existingSnapshotId)).thenReturn(mockSnapshot);
        // Set up mocks to prevent rangeCheck from throwing for other reasons
        when(snapshotManager.earliestSnapshotId()).thenReturn(1L);
        when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(4L);

        fetcher = new NextSnapshotFetcher(snapshotManager, changelogManager, false);

        // Act: Call a few times for a missing snapshot to increment the counter
        fetcher.getNextSnapshot(missingSnapshotId);
        fetcher.getNextSnapshot(missingSnapshotId);

        // Now get an existing snapshot, which should reset the counter
        Snapshot result = fetcher.getNextSnapshot(existingSnapshotId);
        assertThat(result).isNotNull();

        // Assert: Now call for the missing one again RANGE_CHECK_INTERVAL - 1 times.
        // If the counter was reset, it shouldn't throw.
        for (int i = 0; i < RANGE_CHECK_INTERVAL - 1; i++) {
            assertThat(fetcher.getNextSnapshot(missingSnapshotId)).isNull();
        }
        // No exception should be thrown on the 16th total call for the missing ID
        assertThat(fetcher.getNextSnapshot(missingSnapshotId)).isNull();
    }
}
