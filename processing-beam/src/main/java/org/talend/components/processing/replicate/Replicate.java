package org.talend.components.processing.replicate;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import static org.talend.sdk.component.api.component.Icon.IconType.REPLICATE;

@Version
@Icon(REPLICATE)
@Processor(name = "Replicate")
@Documentation("This component replicates the input two one or two outputs (limited to two outputs for the moment).")
public class Replicate extends PTransform<PCollection<IndexedRecord>, PCollectionTuple> {

    private final ReplicateConfiguration configuration;

    private final static String FLOW_CONNECTOR = "__default__";

    private final static String SECOND_FLOW_CONNECTOR = "second";

    public TupleTag<IndexedRecord> defaultTag = new TupleTag<>(FLOW_CONNECTOR);

    public TupleTag<IndexedRecord> secondTag = new TupleTag<>(SECOND_FLOW_CONNECTOR);

    static class ReplicateDoFn extends DoFn<IndexedRecord, IndexedRecord> {

        private Replicate runtime;

        public ReplicateDoFn(Replicate runtime) {
            this.runtime = runtime;
        }

        @ProcessElement
        public void processElement(@Element IndexedRecord record, MultiOutputReceiver out) {
            // broadcast the incoming record to the different outputs
            out.get(runtime.defaultTag).output(record);
            out.get(runtime.secondTag).output(record);
        }
    }

    public Replicate(@Option("configuration") final ReplicateConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject ignored, @Output final OutputEmitter<JsonObject> output,
            @Output(SECOND_FLOW_CONNECTOR) final OutputEmitter<JsonObject> second) {
        // no-op
    }

    @Override
    public PCollectionTuple expand(PCollection<IndexedRecord> input) {
        ReplicateDoFn doFn = new ReplicateDoFn(this);
        return input.apply(ParDo.of(doFn).withOutputTags(defaultTag, TupleTagList.of(secondTag)));
    }
}
