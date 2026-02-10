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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility to load per-partition bucket counts from the latest snapshot's manifest entries.
 *
 * <p>This enables per-partition bucket count awareness for fixed-bucket tables that have undergone
 * rescale operations, allowing different partitions to have different bucket counts.
 */
public class PartitionBucketCountLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketCountLoader.class);

    /**
     * Load per-partition bucket counts from the latest snapshot of the given table.
     *
     * <p>Only partitioned tables can have different bucket counts per partition. For
     * non-partitioned tables, this returns an empty map.
     *
     * @param table the file store table to scan
     * @return a map from partition to its total bucket count, or empty map if not applicable
     */
    public static Map<BinaryRow, Integer> load(FileStoreTable table) {
        if (table.partitionKeys().isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            List<ManifestEntry> entries = table.store().newScan().plan().files();
            Map<BinaryRow, Integer> result = new HashMap<>();
            for (ManifestEntry entry : entries) {
                int totalBuckets = entry.totalBuckets();
                if (totalBuckets > 0) {
                    BinaryRow partition = entry.partition();
                    result.putIfAbsent(partition.copy(), totalBuckets);
                }
            }

            if (!result.isEmpty()) {
                LOG.info(
                        "Loaded per-partition bucket counts for table {}: {} partitions found",
                        table.name(),
                        result.size());
            }
            return result;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to load per-partition bucket counts for table {}, "
                            + "falling back to default bucket count",
                    table.name(),
                    e);
            return Collections.emptyMap();
        }
    }
}
