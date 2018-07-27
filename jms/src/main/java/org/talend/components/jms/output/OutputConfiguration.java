package org.talend.components.jms.output;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.components.jms.service.ActionService.ACTION_LIST_SUPPORTED_BROKER;

@GridLayout(value = {
        @GridLayout.Row({"moduleList"}),
        @GridLayout.Row({"url"}),
        @GridLayout.Row({"userIdentity"}),
        @GridLayout.Row({"userName", "password"}),
        @GridLayout.Row({"userJNDILookup"}),
        @GridLayout.Row({"messageType"}),
        @GridLayout.Row({"to"}),
        @GridLayout.Row({"processingMode"})},
        names = GridLayout.FormType.MAIN)
@GridLayout(value = {
        @GridLayout.Row({"deliveryMode"}),
        @GridLayout.Row({"properties"})},
        names = GridLayout.FormType.ADVANCED)
@Documentation("TODO fill the documentation for this configuration")
@Data
public class OutputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Data type from the supported jms providers list")
    @Proposable(ACTION_LIST_SUPPORTED_BROKER)
    private String moduleList;

    @Option
    @Documentation("Input for server URL")
    private String url = "tcp://host:port";

    @Option
    @Documentation("Checkbox for User Identity Checking")
    private boolean userIdentity = false;

    @Option
    @Documentation("Input for User Name")
    @ActiveIf(target = "userIdentity", value = "true")
    private String userName;

    @Option
    @Credential
    @Documentation("Input for password")
    @ActiveIf(target = "userIdentity", value = "true")
    private String password;

    @Option
    @Documentation("Checkbox for JNDI Name Lookup Destination")
    private boolean userJNDILookup = false;

    @Option
    @Documentation("Drop down list for Message Type")
    private MessageType messageType = MessageType.TOPIC;

    @Option
    @Documentation("Input for TOPIC/QUEUE Name")
    private String to;

    @Option
    @Documentation("Drop down list for Processing Mode")
    private ProcessingMode processingMode = ProcessingMode.RAW_MESSAGE;

    @Option
    @Documentation("Drop down list for Delivery Mode")
    private DeliveryMode deliveryMode = DeliveryMode.NOT_PERSISTENT;

    @Option
    @Documentation("Properties table")
    private List<JMSOutputAdvancedProperties> properties;

    public static enum MessageType {
        QUEUE,
        TOPIC
    }

    public static enum DeliveryMode {
        NOT_PERSISTENT(1),
        PERSISTENT(2);

        private int intValue;

        DeliveryMode(int value) {
            this.intValue = value;
        }

        public int getIntValue() {
            return intValue;
        }
    }

}
