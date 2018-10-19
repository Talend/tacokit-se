package org.talend.components.processing.flatten;

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

import static org.talend.sdk.component.api.component.Icon.IconType.NORMALIZE;

@Version
@Processor(name = "Flatten")
@Icon(NORMALIZE)
@Documentation("Create multiple records from one field of the incoming record.")
public class Flatten extends PTransform<PCollection<IndexedRecord>, PCollection> {

    private FlattenConfiguration configuration;

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        //
    }

    public Flatten(@Option("configuration") final FlattenConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PCollection expand(PCollection<IndexedRecord> inputPCollection) {
        FlattenDoFn doFn = new FlattenDoFn() //
                .withConfiguration(configuration);

        return inputPCollection.apply(ParDo.of(doFn));
    }
}
