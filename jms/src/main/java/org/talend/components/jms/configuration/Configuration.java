package org.talend.components.jms.configuration;

import lombok.Data;
import org.talend.components.jms.output.JMSOutputAdvancedProperties;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static org.talend.components.jms.service.ActionService.ACTION_LIST_SUPPORTED_BROKER;

@Data
public class Configuration implements Serializable {

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
    private String destination;

    @Option
    @Documentation("Properties table")
    private List<JMSOutputAdvancedProperties> properties;

}
