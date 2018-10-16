package org.talend.components.processing.limit;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;
import java.util.concurrent.atomic.AtomicLong;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

@Version
@Processor(name = "Limit")
@Icon(custom = "limit", value = CUSTOM)
@Documentation("This component filters the input with a counter/limit.")
public class Limit extends PTransform<PCollection<IndexedRecord>, PCollection> {

    private LimitConfiguration configuration;

    public static AtomicLong counter = new AtomicLong(0L);

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        //
    }

    public Limit(@Option("configuration") final LimitConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PCollection expand(PCollection<IndexedRecord> inputPCollection) {
        LimitDoFn doFn = new LimitDoFn().withConfiguration(configuration);
        return inputPCollection.apply(ParDo.of(doFn));
    }
}
