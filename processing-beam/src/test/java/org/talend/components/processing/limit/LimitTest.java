// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.processing.limit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link Limit} and {@link LimitDoFn}. Tests are in the same class because they share a common
 * static state, which is re-initialized throughout the test cases. We don't want these tests to be ran concurrently.
 */
public class LimitTest {

    private final Schema inputSimpleSchema = SchemaBuilder.record("inputRow") //
            .fields() //
            .name("a").type().optional().stringType()
            .endRecord();

    private final GenericRecord inputSimpleRecord = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", "aaa")
            .build();

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    /**
     * Demonstrates the basic use case for the {@link Limit}.
     */
    @Test
    public void testBasic() {
        PCollection<IndexedRecord> input = pipeline.apply( //
                Create.<IndexedRecord>of(inputSimpleRecord, inputSimpleRecord, inputSimpleRecord)
        );

        LimitConfiguration configuration = new LimitConfiguration();
        configuration.setLimit(2L);
        Limit runtime = new Limit(configuration);

        PCollection<IndexedRecord> afterLimit = input.apply(runtime);
        PCollection<Long> output = afterLimit.apply(Count.<IndexedRecord>globally());

        PAssert.that(output).containsInAnyOrder(2L);
        pipeline.run().waitUntilFinish();
    }

    /**
     * Test the {@link LimitDoFn}.
     */
    @Test
    public void testDoFn() throws Exception {

        // Process the same number of records than the limit
        DoFnTester<IndexedRecord, IndexedRecord> fnTester1 = DoFnTester.of(createLimitFunction(3L));
        List<IndexedRecord> outputs1 = fnTester1.processBundle(inputSimpleRecord, inputSimpleRecord, inputSimpleRecord);
        assertEquals(3, outputs1.size());

        // Process more records than the limit
        DoFnTester<IndexedRecord, IndexedRecord> fnTester2 = DoFnTester.of(createLimitFunction(2L));
        List<IndexedRecord> outputs2 = fnTester2.processBundle(inputSimpleRecord, inputSimpleRecord, inputSimpleRecord);
        assertEquals(2, outputs2.size());

        // Process less records than the limit
        DoFnTester<IndexedRecord, IndexedRecord> fnTester3 = DoFnTester.of(createLimitFunction(4L));
        List<IndexedRecord> outputs3 = fnTester3.processBundle(inputSimpleRecord, inputSimpleRecord, inputSimpleRecord);
        assertEquals(3, outputs3.size());
    }

    private LimitDoFn createLimitFunction(Long limit) {
        LimitConfiguration configuration = new LimitConfiguration();
        configuration.setLimit(limit);
        // Reset the static counter between each execution
        Limit.counter = new AtomicLong(0L);
        return new LimitDoFn().withConfiguration(configuration);
    }
}
