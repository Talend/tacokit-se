package org.talend.components.processing.python;

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
import static org.talend.sdk.component.api.component.Icon.IconType.PYTHON;

@Version
@Processor(name = "Python")
@Icon(PYTHON)
@Documentation("This component execute python code on incoming data.")
public class Python extends PTransform<PCollection<IndexedRecord>, PCollection> {

    private PythonConfiguration configuration;

    public Python(@Option("configuration") final PythonConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        //
    }

    @Override
    public PCollection expand(PCollection<IndexedRecord> inputPCollection) {
        PythonDoFn doFn = new PythonDoFn().withConfiguration(configuration);
        return inputPCollection.apply(ParDo.of(doFn));
    }
}
