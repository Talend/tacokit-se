/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.rabbitmq.source;

import lombok.Data;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }),
        @GridLayout.Row({ "maximumMessages" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "basicConfig" }) }, names = GridLayout.FormType.ADVANCED)
@Documentation("Main configuration class for RabbitMQInput component")
@Data
public class InputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Common configuration")
    private BasicConfiguration basicConfig;

    @Option
    @Min(0)
    @Documentation("Maximum messages defines a number of messages this component will listen to. "
            + "After reaching the maximum component will stop receiving messages")
    private Integer maximumMessages = 100;

}