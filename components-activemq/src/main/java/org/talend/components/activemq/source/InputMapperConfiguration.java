/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.activemq.source;

import lombok.Data;
import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }), @GridLayout.Row({ "subscriptionConfig" }),
        @GridLayout.Row({ "timeout" }), @GridLayout.Row({ "maximumMessages" }),
        @GridLayout.Row({ "messageSelector" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "basicConfig" }) }, names = GridLayout.FormType.ADVANCED)
@Documentation("Main configuration class for JMSInput component")
@Data
public class InputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Common basicConfig")
    private BasicConfiguration basicConfig;

    @Option
    @Documentation("Durable subscription configuration")
    @ActiveIf(target = "basicConfig/messageType", value = "TOPIC")
    private DurableSubscriptionConfiguration subscriptionConfig;

    @Option
    @Min(0)
    @Documentation("JMS receive message timeout. A timeout of zero never expires, and the call blocks indefinitely.")
    private Integer timeout = 0;

    @Option
    @Min(0)
    @Documentation("Maximum messages defines a number of messages this component will listen to. "
            + "After reaching the maximum component will stop receiving messages")
    private Integer maximumMessages = 1000;

    @Option
    @Documentation("Message Selector Expression used to receive only messages whose headers and properties match the selector")
    private String messageSelector;

}