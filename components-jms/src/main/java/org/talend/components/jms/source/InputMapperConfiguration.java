// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jms.source;

import lombok.Data;
import org.talend.components.jms.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }), @GridLayout.Row({ "subscriptionConfig" }),
        @GridLayout.Row({ "timeout" }), @GridLayout.Row({ "maximumMessages" }),
        @GridLayout.Row({ "messageSelector" }) }, names = GridLayout.FormType.MAIN)
@Documentation("Main configuration class for JMSInput component")
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
    @Required
    @Min(0)
    @Documentation("JMS receive message timeout. A timeout of zero never expires, and the call blocks indefinitely.")
    private Integer timeout;

    @Option
    @Required
    @Min(0)
    @Documentation("Maximum messages defines a number of messages this component will listen to. "
            + "After reaching the maximum component will stop receiving messages")
    private Integer maximumMessages;

    @Option
    @Documentation("Message Selector Expression used to receive only messages whose headers and properties match the selector")
    private String messageSelector;

}