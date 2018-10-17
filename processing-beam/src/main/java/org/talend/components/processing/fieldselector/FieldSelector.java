package org.talend.components.processing.fieldselector;

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

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.FIELD_SELECTOR;

@Version
@Icon(FIELD_SELECTOR)
@Processor(name = "FieldSelector")
@Documentation("Create an output with only specific selected fields.")
public class FieldSelector extends PTransform<PCollection<IndexedRecord>, PCollection> implements Serializable {

    private final FieldSelectorConfiguration configuration;

    public FieldSelector(@Option("configuration") final FieldSelectorConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final IndexedRecord ignored, @Output final OutputEmitter<IndexedRecord> output) {
        // Dummy method to pass validation
        // do not use
    }

    @Override
    public PCollection expand(PCollection<IndexedRecord> inputPCollection) {
        FieldSelectorDoFn doFn = new FieldSelectorDoFn().withConfiguration(configuration);

        return inputPCollection.apply(ParDo.of(doFn));
    }

}
