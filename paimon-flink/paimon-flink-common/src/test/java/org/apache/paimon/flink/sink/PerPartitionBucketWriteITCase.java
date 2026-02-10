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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionBucketCountLoader;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for per-partition bucket count support.
 *
 * <p>Validates that:
 *
 * <ul>
 *   <li>Partitioned tables allow different bucket counts per partition after rescale
 *   <li>Non-partitioned tables still reject bucket count mismatches
 *   <li>PartitionBucketCountLoader correctly loads per-partition bucket counts
 *   <li>Each partition always maintains its expected bucket count
 * </ul>
 */
public class PerPartitionBucketWriteITCase extends CatalogITCaseBase {

    @Test
    public void testPartitionedTableAllowsDifferentBucketCounts() throws Exception {
        sql(
                "CREATE TABLE T1 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        sql("INSERT INTO T1 VALUES (1, 1, 100)");
        assertBucketsForPartition("T1", 1, 4);

        // Rescale to 8 buckets and overwrite only partition 2
        sql("ALTER TABLE T1 SET ('bucket' = '8')");
        sql("INSERT OVERWRITE T1 PARTITION (pt = 2) SELECT 1, 200");

        // Partition 1 keeps 4 buckets, partition 2 gets 8 buckets
        assertBucketsForPartition("T1", 1, 4);
        assertBucketsForPartition("T1", 2, 8);

        // Writing to partition 1 (old bucket count) should succeed
        sql("INSERT INTO T1 VALUES (1, 2, 101)");

        // Bucket counts remain unchanged after additional writes
        assertBucketsForPartition("T1", 1, 4);
        assertBucketsForPartition("T1", 2, 8);

        List<Row> result = sql("SELECT * FROM T1 ORDER BY pt, k");
        assertThat(result).containsExactly(Row.of(1, 1, 100), Row.of(1, 2, 101), Row.of(2, 1, 200));
    }

    @Test
    public void testNonPartitionedTableRejectsDifferentBucketCounts() {
        sql(
                "CREATE TABLE T2 (k INT, v BIGINT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ('bucket' = '2')");
        sql("INSERT INTO T2 VALUES (1, 100)");

        sql("ALTER TABLE T2 SET ('bucket' = '4')");

        assertThatThrownBy(() -> sql("INSERT INTO T2 VALUES (2, 200)"))
                .rootCause()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("new bucket num")
                .hasMessageContaining("INSERT OVERWRITE");
    }

    @Test
    public void testPartitionedTableWriteAcrossMultiplePartitions() throws Exception {
        sql(
                "CREATE TABLE T3 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        sql("INSERT INTO T3 VALUES (1, 1, 100), (2, 1, 200), (3, 1, 300)");
        assertBucketsForPartition("T3", 1, 4);
        assertBucketsForPartition("T3", 2, 4);
        assertBucketsForPartition("T3", 3, 4);

        // Change bucket count to 8 (metadata only, no overwrite)
        sql("ALTER TABLE T3 SET ('bucket' = '8')");

        // Writing across all partitions with old bucket count should work
        sql("INSERT INTO T3 VALUES (1, 2, 101), (2, 2, 201), (3, 2, 301)");

        // All partitions still have 4 buckets
        assertBucketsForPartition("T3", 1, 4);
        assertBucketsForPartition("T3", 2, 4);
        assertBucketsForPartition("T3", 3, 4);

        List<Row> result = sql("SELECT * FROM T3 ORDER BY pt, k");
        assertThat(result)
                .containsExactly(
                        Row.of(1, 1, 100),
                        Row.of(1, 2, 101),
                        Row.of(2, 1, 200),
                        Row.of(2, 2, 201),
                        Row.of(3, 1, 300),
                        Row.of(3, 2, 301));
    }

    @Test
    public void testWriteToNewPartitionAfterRescale() throws Exception {
        sql(
                "CREATE TABLE T4 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        sql("INSERT INTO T4 VALUES (1, 1, 100)");
        assertBucketsForPartition("T4", 1, 4);

        sql("ALTER TABLE T4 SET ('bucket' = '8')");

        // New partition gets the new bucket count
        sql("INSERT INTO T4 VALUES (2, 1, 200)");
        assertBucketsForPartition("T4", 1, 4);
        assertBucketsForPartition("T4", 2, 8);

        List<Row> result = sql("SELECT * FROM T4 ORDER BY pt, k");
        assertThat(result).containsExactly(Row.of(1, 1, 100), Row.of(2, 1, 200));
    }

    @Test
    public void testCompactionWithDifferentBucketCountsPerPartition() throws Exception {
        sql(
                "CREATE TABLE T_COMPACT (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        // Write multiple batches to partition 1 to create multiple sorted runs
        sql("INSERT INTO T_COMPACT VALUES (1, 1, 100), (1, 2, 200)");
        sql("INSERT INTO T_COMPACT VALUES (1, 1, 101), (1, 3, 300)");
        assertBucketsForPartition("T_COMPACT", 1, 4);

        // Rescale to 8 buckets, overwrite partition 2
        sql("ALTER TABLE T_COMPACT SET ('bucket' = '8')");
        sql("INSERT OVERWRITE T_COMPACT PARTITION (pt = 2) SELECT 1, 400");
        sql("INSERT INTO T_COMPACT VALUES (2, 2, 500)");
        assertBucketsForPartition("T_COMPACT", 1, 4);
        assertBucketsForPartition("T_COMPACT", 2, 8);

        // Compact all partitions — should work despite different bucket counts
        sql("CALL sys.compact(`table` => 'default.T_COMPACT')");

        // Bucket counts should be preserved after compaction
        assertBucketsForPartition("T_COMPACT", 1, 4);
        assertBucketsForPartition("T_COMPACT", 2, 8);

        // Data should be correct after compaction (k=1 in pt=1 was updated from 100 to 101)
        List<Row> result = sql("SELECT * FROM T_COMPACT ORDER BY pt, k");
        assertThat(result)
                .containsExactly(
                        Row.of(1, 1, 101),
                        Row.of(1, 2, 200),
                        Row.of(1, 3, 300),
                        Row.of(2, 1, 400),
                        Row.of(2, 2, 500));
    }

    @Test
    public void testCompactionPerPartitionPreservesBucketCounts() throws Exception {
        sql(
                "CREATE TABLE T_COMPACT2 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        // Write to partitions 1 and 2 with 4 buckets
        sql("INSERT INTO T_COMPACT2 VALUES (1, 1, 100), (2, 1, 200)");
        sql("INSERT INTO T_COMPACT2 VALUES (1, 1, 101), (2, 1, 201)");

        // Rescale to 8 and write to partition 3
        sql("ALTER TABLE T_COMPACT2 SET ('bucket' = '8')");
        sql("INSERT INTO T_COMPACT2 VALUES (3, 1, 300)");
        sql("INSERT INTO T_COMPACT2 VALUES (3, 1, 301)");

        assertBucketsForPartition("T_COMPACT2", 1, 4);
        assertBucketsForPartition("T_COMPACT2", 2, 4);
        assertBucketsForPartition("T_COMPACT2", 3, 8);

        // Compact only partition 1
        sql("CALL sys.compact(`table` => 'default.T_COMPACT2', partitions => 'pt=1')");
        assertBucketsForPartition("T_COMPACT2", 1, 4);

        // Compact only partition 3
        sql("CALL sys.compact(`table` => 'default.T_COMPACT2', partitions => 'pt=3')");
        assertBucketsForPartition("T_COMPACT2", 3, 8);

        // All bucket counts preserved
        assertBucketsForPartition("T_COMPACT2", 1, 4);
        assertBucketsForPartition("T_COMPACT2", 2, 4);
        assertBucketsForPartition("T_COMPACT2", 3, 8);

        // Verify data correctness (latest values after merge)
        List<Row> result = sql("SELECT * FROM T_COMPACT2 ORDER BY pt, k");
        assertThat(result)
                .containsExactly(Row.of(1, 1, 101), Row.of(2, 1, 201), Row.of(3, 1, 301));
    }

    @Test
    public void testPartitionBucketCountLoaderEmptyTable() throws Exception {
        sql(
                "CREATE TABLE T5 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");

        assertThat(loadBucketCounts("T5")).isEmpty();
    }

    @Test
    public void testPartitionBucketCountLoaderNonPartitioned() throws Exception {
        sql(
                "CREATE TABLE T6 (k INT, v INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ('bucket' = '4')");
        sql("INSERT INTO T6 VALUES (1, 100)");

        assertThat(loadBucketCounts("T6")).isEmpty();
    }

    @Test
    public void testPartitionBucketCountLoaderSinglePartition() throws Exception {
        sql(
                "CREATE TABLE T7 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");
        sql("INSERT INTO T7 VALUES (1, 1, 100), (1, 2, 200)");

        assertBucketsForPartition("T7", 1, 4);
    }

    @Test
    public void testPartitionBucketCountLoaderMultiplePartitions() throws Exception {
        sql(
                "CREATE TABLE T8 (pt INT, k INT, v INT, PRIMARY KEY (pt, k) NOT ENFORCED)"
                        + " PARTITIONED BY (pt) WITH ('bucket' = '4')");
        sql("INSERT INTO T8 VALUES (1, 1, 100), (2, 1, 200), (3, 1, 300)");

        assertBucketsForPartition("T8", 1, 4);
        assertBucketsForPartition("T8", 2, 4);
        assertBucketsForPartition("T8", 3, 4);
    }

    // --- Helpers ---

    private Map<BinaryRow, Integer> loadBucketCounts(String tableName) throws Exception {
        FileStoreTable table = paimonTable(tableName);
        return PartitionBucketCountLoader.load(table);
    }

    /** Assert that a specific partition has the expected number of buckets. */
    private void assertBucketsForPartition(
            String tableName, int partitionValue, int expectedBuckets) throws Exception {
        Map<BinaryRow, Integer> bucketCounts = loadBucketCounts(tableName);
        BinaryRow partitionKey = partitionRow(partitionValue);
        assertThat(bucketCounts)
                .as("partition pt=%d should have %d buckets", partitionValue, expectedBuckets)
                .containsEntry(partitionKey, expectedBuckets);
    }

    private static BinaryRow partitionRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
