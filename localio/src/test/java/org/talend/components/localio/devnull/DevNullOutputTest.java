package org.talend.components.localio.devnull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.junit.jupiter.api.Test;
import org.talend.daikon.avro.GenericDataRecordHelper;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.beam.coder.registry.SchemaRegistryCoder;
import org.talend.sdk.component.runtime.beam.spi.record.AvroRecord;
import org.talend.sdk.component.runtime.beam.transform.RecordNormalizer;
import org.talend.sdk.component.runtime.output.Processor;
import java.util.Optional;

import static java.util.Collections.emptyMap;

@WithComponents("org.talend.components.localio")
class DevNullOutputTest {

    @Injected
    private ComponentsHandler handler;

    @Test
    void ensureItRuns() {
        final Pipeline pipeline = Pipeline.create();
        final Optional<Processor> processor = handler.asManager().findProcessor("LocalIO", "DevNullOutputRuntime", 1, emptyMap());

        final PTransform<PCollection<Record>, PDone> input = processor.map(TalendIO::write)
                .orElseThrow(() -> new IllegalArgumentException("No component for fixed flow input"));

        pipeline.apply(Create.of((Record) new AvroRecord(GenericDataRecordHelper.createRecord(new Object[] { "a", 1 })))
                .withCoder(SchemaRegistryCoder.of())).apply("Normalizer", RecordNormalizer.of(processor.get().plugin()))
                .apply(input);
        pipeline.run().waitUntilFinish();
    }
}
