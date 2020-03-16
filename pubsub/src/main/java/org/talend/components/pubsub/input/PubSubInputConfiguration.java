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
package org.talend.components.pubsub.input;

import lombok.Data;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout(names = GridLayout.FormType.MAIN, value = { @GridLayout.Row("dataSet"), @GridLayout.Row("consumeMsg") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("pullMode"), @GridLayout.Row("maxMsg") })
@Documentation("Configuration for Subscriber")
public class PubSubInputConfiguration implements Serializable {

    @Option
    @Documentation("DataSet")
    private PubSubDataSet dataSet;

    @Option
    @Documentation("Deliver message to the subscriber only once")
    private boolean consumeMsg = false;

    @Option
    @Documentation(("Pull mode : synchronous or asynchronous"))
    @DefaultValue("SYNCHRONOUS")
    private PullMode pullMode = PullMode.SYNCHRONOUS;

    @Option
    @ActiveIf(target = "pullMode", value = "SYNCHRONOUS")
    @DefaultValue("100")
    @Documentation("Maximum number of messages received per request")
    private int maxMsg = 100;

    public static enum PullMode {
        SYNCHRONOUS,
        ASYNCHRONOUS
    }
}
