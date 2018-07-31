package org.talend.components.jms.source;

import lombok.Data;
import org.talend.components.jms.configuration.Configuration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout(value = {
        @GridLayout.Row({"moduleList"}),
        @GridLayout.Row({"url"}),
        @GridLayout.Row({"userIdentity"}),
        @GridLayout.Row({"userName", "password"}),
        @GridLayout.Row({"subscriptionConfiguration"}),
        @GridLayout.Row({"userJNDILookup"}),
        @GridLayout.Row({"messageType"}),
        @GridLayout.Row({"destination"}),
        @GridLayout.Row({"timeout", "maximumMessages"}),
        @GridLayout.Row({"messageSelector"}),
        @GridLayout.Row({"processingMode"})},
        names = GridLayout.FormType.MAIN)
@GridLayout(value = {
        @GridLayout.Row({"properties"})},
        names = GridLayout.FormType.ADVANCED)
@Documentation("TODO fill the documentation for this configuration")
@Data
public class InputMapperConfiguration extends Configuration {

    @Option
    @Documentation("Durable subscription configuration")
    @ActiveIf(target = "messageType", value = "TOPIC")
    private DurableSubscriptionConfiguration subscriptionConfiguration;

    @Option
    @Documentation("Timeout")
    private Integer timeout = -1;

    @Option
    @Documentation("Maximum messages")
    private Integer maximumMessages = -1;

    @Option
    @Documentation("Message Selector Expression")
    private Integer messageSelector;

}