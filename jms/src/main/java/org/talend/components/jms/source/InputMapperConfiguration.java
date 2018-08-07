package org.talend.components.jms.source;

import lombok.Data;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.components.jms.output.JMSOutputAdvancedProperties;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@DataSet("JMSInputDataSet")
@GridLayout(value = {
        @GridLayout.Row({"basicConfig"}),
        @GridLayout.Row({"subscriptionConfig"}),
        @GridLayout.Row({"timeout", "maximumMessages"}),
        @GridLayout.Row({"messageSelector"})},
        names = GridLayout.FormType.MAIN)
@GridLayout(value = {@GridLayout.Row({"schema"}),
        @GridLayout.Row({"properties"})},
        names = GridLayout.FormType.ADVANCED)
@Documentation("TODO fill the documentation for this basicConfig")
@Data
public class InputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Common basicConfig")
    private BasicConfiguration basicConfig;

    @Option
    @Documentation("Durable subscription configuration")
    @ActiveIf(target = "basicConfig.messageType", value = "TOPIC")
    private DurableSubscriptionConfiguration subscriptionConfig;

    @Option
    @Documentation("JMS receive message timeout. A timeout of zero never expires, and the call blocks indefinitely.")
    private Integer timeout = -1;

    @Option
    @Documentation("Maximum messages defines a number of messages this component will listen to. "
            + "After reaching the maximum component will stop receiving messages")
    private Integer maximumMessages = -1;

    @Option
    @Documentation("Message Selector Expression used to receive only messages whose headers and properties match the selector")
    private String messageSelector = "";

    @Option
    @Structure(type = Structure.Type.OUT, discoverSchema = "discoverSchema")
    @Documentation("Guess schema")
    private List<String> schema;

    @Option
    @Documentation("Properties table")
    private List<JMSOutputAdvancedProperties> properties;

}