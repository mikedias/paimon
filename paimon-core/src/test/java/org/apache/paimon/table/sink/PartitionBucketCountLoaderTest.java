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

package org.apache.paimon.table.sink;

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PartitionBucketCountLoader}. */
public class PartitionBucketCountLoaderTest {

    @Test
    public void testNonPartitionedTableReturnsEmptyMap() {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.partitionKeys()).thenReturn(Collections.emptyList());

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).isEmpty();
    }

    @Test
    public void testEmptyTableReturnsEmptyMap() {
        FileStoreTable table = mockPartitionedTable(Collections.emptyList());

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).isEmpty();
    }

    @Test
    public void testMultiplePartitionsSameBucketCount() {
        BinaryRow partition1 = createPartition(1);
        BinaryRow partition2 = createPartition(2);
        BinaryRow partition3 = createPartition(3);

        FileStoreTable table =
                mockPartitionedTable(
                        Arrays.asList(
                                mockEntry(partition1, 4),
                                mockEntry(partition2, 4),
                                mockEntry(partition3, 4)));

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).hasSize(3);
        assertThat(result.get(partition1)).isEqualTo(4);
        assertThat(result.get(partition2)).isEqualTo(4);
        assertThat(result.get(partition3)).isEqualTo(4);
    }

    @Test
    public void testMultiplePartitionsDifferentBucketCounts() {
        BinaryRow partition1 = createPartition(1);
        BinaryRow partition2 = createPartition(2);

        FileStoreTable table =
                mockPartitionedTable(
                        Arrays.asList(mockEntry(partition1, 4), mockEntry(partition2, 8)));

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).hasSize(2);
        assertThat(result.get(partition1)).isEqualTo(4);
        assertThat(result.get(partition2)).isEqualTo(8);
    }

    @Test
    public void testEntriesWithZeroBucketsAreIgnored() {
        BinaryRow partition1 = createPartition(1);
        BinaryRow partition2 = createPartition(2);

        ManifestEntry entry1 = mockEntry(partition1, 0); // zero buckets, should be ignored
        ManifestEntry entry2 = mockEntry(partition2, 4);

        FileStoreTable table = mockPartitionedTable(Arrays.asList(entry1, entry2));

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).hasSize(1);
        assertThat(result).doesNotContainKey(partition1);
        assertThat(result.get(partition2)).isEqualTo(4);
    }

    @Test
    public void testNegativeBucketsAreIgnored() {
        BinaryRow partition1 = createPartition(1);

        ManifestEntry entry = mockEntry(partition1, -1);

        FileStoreTable table = mockPartitionedTable(Collections.singletonList(entry));

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).isEmpty();
    }

    @Test
    public void testScanExceptionReturnsEmptyMap() {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
        when(table.name()).thenReturn("test_table");

        FileStore<?> store = mock(FileStore.class);
        doReturn(store).when(table).store();
        when(store.newScan()).thenThrow(new RuntimeException("Simulated scan failure"));

        Map<BinaryRow, Integer> result = PartitionBucketCountLoader.load(table);
        assertThat(result).isEmpty();
    }

    // --- Helper methods ---

    private static BinaryRow createPartition(int partValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partValue);
        writer.complete();
        return row;
    }

    private static ManifestEntry mockEntry(BinaryRow partition, int totalBuckets) {
        ManifestEntry entry = mock(ManifestEntry.class);
        when(entry.partition()).thenReturn(partition);
        when(entry.totalBuckets()).thenReturn(totalBuckets);
        return entry;
    }

    private static FileStoreTable mockPartitionedTable(
            java.util.List<ManifestEntry> manifestEntries) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
        when(table.name()).thenReturn("test_table");

        FileStore<?> store = mock(FileStore.class);
        doReturn(store).when(table).store();

        FileStoreScan scan = mock(FileStoreScan.class);
        when(store.newScan()).thenReturn(scan);

        FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);
        when(scan.plan()).thenReturn(plan);
        when(plan.files()).thenReturn(manifestEntries);

        return table;
    }
}
