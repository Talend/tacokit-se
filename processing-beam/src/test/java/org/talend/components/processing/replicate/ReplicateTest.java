package org.talend.components.processing.replicate;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.jupiter.api.Test;
import org.talend.components.adapter.beam.coders.LazyAvroCoder;
import org.talend.daikon.avro.GenericDataRecordHelper;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

class ReplicateTest {

    @Test
    void replicate() {
        // Create pipeline
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        final Pipeline p = Pipeline.create(options);

        // Create PCollection for test
        Schema a = GenericDataRecordHelper.createSchemaFromObject("a", new Object[] { "a" });
        IndexedRecord irA = GenericDataRecordHelper.createRecord(a, new Object[] { "a" });
        IndexedRecord irB = GenericDataRecordHelper.createRecord(a, new Object[] { "b" });
        IndexedRecord irC = GenericDataRecordHelper.createRecord(a, new Object[] { "c" });

        List<IndexedRecord> data = Arrays.asList( //
                irA, //
                irB, //
                irC, //
                irA, //
                irA, //
                irC //
        );

        Replicate processor = new Replicate(new ReplicateConfiguration());

        PCollection<IndexedRecord> input = (PCollection<IndexedRecord>) p.apply(Create.of(data).withCoder(LazyAvroCoder.of()));
        PCollectionTuple tuple = input.apply(processor);
        assertEquals(2, tuple.getAll().size());
        tuple.getAll().forEach((k, v) -> {
            PCollection<IndexedRecord> collection = (PCollection<IndexedRecord>) v;
            collection.setCoder(LazyAvroCoder.of());
            PAssert.that(collection).containsInAnyOrder(irA, irB, irC);
        });

    }

}
