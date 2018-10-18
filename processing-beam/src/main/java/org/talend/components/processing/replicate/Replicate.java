package org.talend.components.processing.replicate;

import org.apache.beam.sdk.values.PCollection;
import org.talend.components.adapter.beam.BeamJobBuilder;
import org.talend.components.adapter.beam.BeamJobContext;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;
import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.REPLICATE;

@Version
@Icon(REPLICATE)
@Processor(name = "Replicate")
@Documentation("This component replicates the input two one or two outputs (limited to two outputs for the moment).")
public class Replicate implements BeamJobBuilder, Serializable {

    private final ReplicateConfiguration configuration;

    private final static String MAIN_CONNECTOR = "__default__";

    // TODO: It would be really useful if we could differentiate between named outputs for the
    // component. For the moment this works because we want the two to be exactly the same in all
    // cases.
    private final static String FLOW_CONNECTOR = "__default__";

    private final static String SECOND_FLOW_CONNECTOR = "second";

    private boolean hasFlow;

    private boolean hasSecondFlow;

    public Replicate(@Option("configuration") final ReplicateConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject ignored, @Output final OutputEmitter<JsonObject> output,
            @Output("second") final OutputEmitter<JsonObject> second) {
        // Dummy method to pass validate
        // no-op
    }

    @Override
    public void build(BeamJobContext beamJobContext) {
        String mainLink = beamJobContext.getLinkNameByPortName("input_" + MAIN_CONNECTOR);
        if (!isEmpty(mainLink)) {
            PCollection<Object> mainPCollection = beamJobContext.getPCollectionByLinkName(mainLink);
            if (mainPCollection != null) {
                String flowLink = beamJobContext.getLinkNameByPortName("output_" + FLOW_CONNECTOR);
                String secondFlowLink = beamJobContext.getLinkNameByPortName("output_" + SECOND_FLOW_CONNECTOR);

                hasFlow = !isEmpty(flowLink);
                hasSecondFlow = !isEmpty(secondFlowLink);

                if (hasFlow) {
                    beamJobContext.putPCollectionByLinkName(flowLink, mainPCollection);
                }
                if (hasSecondFlow) {
                    beamJobContext.putPCollectionByLinkName(secondFlowLink, mainPCollection);
                }
            }
        }
    }

    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }
}
