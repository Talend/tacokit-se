package org.talend.components.jms.configuration;

import lombok.Data;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout(value = {
        @GridLayout.Row({"connection"}),
        @GridLayout.Row({"userJNDILookup"}),
        @GridLayout.Row({"messageType"}),
        @GridLayout.Row({"destination"})},
        names = GridLayout.FormType.MAIN)
public class BasicConfiguration implements Serializable {

    @Option
    @Documentation("JMS connection information")
    private JmsDataStore connection;

    @Option
    @Documentation("Checkbox for JNDI Name Lookup Destination")
    private boolean userJNDILookup = false;

    @Option
    @Documentation("Drop down list for Message Type")
    private MessageType messageType = MessageType.TOPIC;

    @Option
    @Documentation("Input for TOPIC/QUEUE Name")
    private String destination;
}
