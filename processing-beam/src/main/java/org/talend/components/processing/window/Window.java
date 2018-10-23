package org.talend.components.processing.window;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.CommonErrorCodes;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import static org.talend.sdk.component.api.component.Icon.IconType.WINDOW;

@Version
@Icon(WINDOW)
@Processor(name = "Window")
@Documentation("This component makes windows of data by buffering input records according to provided duration(s).")
public class Window extends PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>> {

    private final WindowConfiguration configuration;

    public Window(@Option("configuration") final WindowConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        //
    }

    @Override
    public PCollection<IndexedRecord> expand(PCollection<IndexedRecord> input) {
        PCollection<IndexedRecord> windowed_items;

        if (configuration.getWindowLength() < 1) {
            TalendRuntimeException.build(CommonErrorCodes.UNEXPECTED_ARGUMENT).setAndThrow("windowLength",
                    String.valueOf(configuration.getWindowLength()));
        }

        if (configuration.getWindowSession()) {
            // Session Window
            windowed_items = input.apply(org.apache.beam.sdk.transforms.windowing.Window
                    .into(Sessions.withGapDuration(Duration.millis(configuration.getWindowLength()))));
        } else {
            if (configuration.getWindowSlideLength() < 1) {
                // Fixed Window
                windowed_items = input.apply(org.apache.beam.sdk.transforms.windowing.Window
                        .into(FixedWindows.of(new Duration(configuration.getWindowLength().longValue()))));
            } else {
                // Sliding Window
                windowed_items = input.apply(org.apache.beam.sdk.transforms.windowing.Window
                        .into(SlidingWindows.of(new Duration(configuration.getWindowLength().longValue()))
                                .every(new Duration(configuration.getWindowSlideLength().longValue()))));
            }
        }
        return windowed_items;
    }
}
