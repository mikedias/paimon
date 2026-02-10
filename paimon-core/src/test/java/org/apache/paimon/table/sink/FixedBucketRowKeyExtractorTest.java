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

import org.apache.paimon.bucket.DefaultBucketFunction;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FixedBucketRowKeyExtractor}. */
public class FixedBucketRowKeyExtractorTest {

    @Test
    public void testInvalidBucket() {
        assertThatThrownBy(() -> extractor("n", "b"))
                .hasMessageContaining("Field names [a, b, c] should contains all bucket keys [n].");

        assertThatThrownBy(() -> extractor("a", "b"))
                .hasMessageContaining("Primary keys [b] should contains all bucket keys [a].");

        assertThatThrownBy(() -> extractor("a", "a", "a,b"))
                .hasMessageContaining("Bucket keys [a] should not in partition keys [a].");
    }

    @Test
    public void testBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThat(bucket(extractor("a", "a,b"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a"), row)).isEqualTo(96);
        assertThat(bucket(extractor("", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("a,b", "a,b"), row)).isEqualTo(27);
        assertThat(bucket(extractor("a,b,c", ""), row)).isEqualTo(40);
        assertThat(bucket(extractor("", "a,b,c"), row)).isEqualTo(40);
    }

    @Test
    public void testIllegalBucket() {
        GenericRow row = GenericRow.of(5, 6, 7);
        assertThatThrownBy(() -> bucket(extractor("", "", "a", -1), row));
    }

    @Test
    public void testUnCompactDecimalAndTimestampNullValueBucketNumber() {
        GenericRow row = GenericRow.of(null, null, null, 1);
        int bucketNum = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "d", new DecimalType(38, 18)),
                                new DataField(1, "ltz", new LocalZonedTimestampType()),
                                new DataField(2, "ntz", new TimestampType()),
                                new DataField(3, "k", new IntType())));

        String[] bucketColsToTest = {"d", "ltz", "ntz"};
        DefaultBucketFunction bucketFunction = new DefaultBucketFunction();
        for (String bucketCol : bucketColsToTest) {
            FixedBucketRowKeyExtractor extractor = extractor(rowType, "", bucketCol, "", bucketNum);
            BinaryRow binaryRow =
                    new InternalRowSerializer(rowType.project(bucketCol)).toBinaryRow(row);
            assertThat(bucket(extractor, row))
                    .isEqualTo(bucketFunction.bucket(binaryRow, bucketNum));
        }
    }

    @Test
    public void testPerPartitionBucketCount() {
        // Compute expected values using individual single-bucket-count extractors
        int expectedPt1 = bucketFor(partitionedExtractorWithDefault(4), 1, 6, 7);
        int expectedPt2 = bucketFor(partitionedExtractorWithDefault(8), 2, 6, 7);
        int expectedPt3 = bucketFor(partitionedExtractorWithDefault(100), 3, 6, 7);

        FixedBucketRowKeyExtractor extractor = partitionedExtractor(partitionBuckets(4, 8));

        assertThat(bucketFor(extractor, 1, 6, 7)).isEqualTo(expectedPt1);
        assertThat(bucketFor(extractor, 2, 6, 7)).isEqualTo(expectedPt2);
        // Partition 3 has no override — falls back to default (100)
        assertThat(bucketFor(extractor, 3, 6, 7)).isEqualTo(expectedPt3);
    }

    @Test
    public void testPerPartitionBucketCountConsistency() {
        FixedBucketRowKeyExtractor extractor = partitionedExtractor(partitionBuckets(4));

        int first = bucketFor(extractor, 1, 42, 99);
        int second = bucketFor(extractor, 1, 42, 99);
        assertThat(first).isEqualTo(second);
    }

    @Test
    public void testPerPartitionOverrideVsDefault() {
        // Same bucket key but different numBuckets produces different bucket assignments
        int with4 = bucketFor(partitionedExtractorWithDefault(4), 1, 42, 99);
        int with100 = bucketFor(partitionedExtractorWithDefault(100), 1, 42, 99);

        FixedBucketRowKeyExtractor withOverride = partitionedExtractor(partitionBuckets(4));
        FixedBucketRowKeyExtractor withDefault = partitionedExtractor(null);

        assertThat(bucketFor(withOverride, 1, 42, 99)).isEqualTo(with4);
        assertThat(bucketFor(withDefault, 1, 42, 99)).isEqualTo(with100);
    }

    @Test
    public void testNullAndEmptyMapProduceSameBucket() {
        FixedBucketRowKeyExtractor withNull = partitionedExtractor(null);
        FixedBucketRowKeyExtractor withEmpty = partitionedExtractor(new HashMap<>());

        assertThat(bucketFor(withNull, 5, 6, 7)).isEqualTo(bucketFor(withEmpty, 5, 6, 7));
    }

    /** Compute bucket for a row with given field values using the extractor. */
    private int bucketFor(FixedBucketRowKeyExtractor extractor, int a, int b, int c) {
        extractor.setRecord(GenericRow.of(a, b, c));
        return extractor.bucket();
    }

    private int bucket(FixedBucketRowKeyExtractor extractor, InternalRow row) {
        extractor.setRecord(row);
        return extractor.bucket();
    }

    /**
     * Build a partition bucket map using an extractor to produce consistent BinaryRow keys.
     * Partition values are 1, 2, ... matching the order of bucketCounts.
     */
    private Map<BinaryRow, Integer> partitionBuckets(int... bucketCounts) {
        TableSchema schema = partitionedSchema();
        FixedBucketRowKeyExtractor temp = new FixedBucketRowKeyExtractor(schema, null);
        Map<BinaryRow, Integer> map = new HashMap<>();
        for (int i = 0; i < bucketCounts.length; i++) {
            temp.setRecord(GenericRow.of(i + 1, 0, 0));
            map.put(temp.partition().copy(), bucketCounts[i]);
        }
        return map;
    }

    /** Create a partitioned extractor (partKey=a, bucketKey=b, pk=a,b, default bucket=100). */
    private FixedBucketRowKeyExtractor partitionedExtractor(
            Map<BinaryRow, Integer> partitionBucketCounts) {
        return new FixedBucketRowKeyExtractor(partitionedSchema(), partitionBucketCounts);
    }

    /** Create a partitioned extractor with a specific default bucket count and no overrides. */
    private FixedBucketRowKeyExtractor partitionedExtractorWithDefault(int defaultBuckets) {
        return new FixedBucketRowKeyExtractor(schemaOf("a", "b", "a,b", defaultBuckets), null);
    }

    private TableSchema partitionedSchema() {
        return schemaOf("a", "b", "a,b", 100);
    }

    private FixedBucketRowKeyExtractor extractor(String bk, String pk) {
        return extractor("", bk, pk);
    }

    private FixedBucketRowKeyExtractor extractor(String partK, String bk, String pk) {
        return extractor(partK, bk, pk, 100);
    }

    private FixedBucketRowKeyExtractor extractor(
            String partK, String bk, String pk, int numBucket) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        return extractor(rowType, partK, bk, pk, numBucket);
    }

    private FixedBucketRowKeyExtractor extractor(
            RowType rowType, String partK, String bk, String pk, int numBucket) {
        List<DataField> fields = TableSchema.newFields(rowType);
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_KEY.key(), bk);
        options.put(BUCKET.key(), String.valueOf(numBucket));
        TableSchema schema =
                new TableSchema(
                        0,
                        fields,
                        RowType.currentHighestFieldId(fields),
                        "".equals(partK)
                                ? Collections.emptyList()
                                : Arrays.asList(partK.split(",")),
                        "".equals(pk) ? Collections.emptyList() : Arrays.asList(pk.split(",")),
                        options,
                        "");
        return new FixedBucketRowKeyExtractor(schema, null);
    }

    private TableSchema schemaOf(String partK, String bk, String pk, int numBucket) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new IntType()),
                                new DataField(2, "c", new IntType())));
        List<DataField> fields = TableSchema.newFields(rowType);
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET_KEY.key(), bk);
        options.put(BUCKET.key(), String.valueOf(numBucket));
        return new TableSchema(
                0,
                fields,
                RowType.currentHighestFieldId(fields),
                "".equals(partK) ? Collections.emptyList() : Arrays.asList(partK.split(",")),
                "".equals(pk) ? Collections.emptyList() : Arrays.asList(pk.split(",")),
                options,
                "");
    }
}
