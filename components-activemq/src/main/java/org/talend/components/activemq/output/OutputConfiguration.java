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
package org.talend.components.activemq.output;

import lombok.Data;
import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "basicConfig" }),
        @GridLayout.Row({ "deliveryMode" }) }, names = GridLayout.FormType.ADVANCED)
@Documentation("Main configuration class for JMSOutput component")
@Data
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("Common basicConfig")
    private BasicConfiguration basicConfig;

    @Option
    @Documentation("Drop down list for Delivery Mode")
    private DeliveryMode deliveryMode = DeliveryMode.NOT_PERSISTENT;

    public static enum DeliveryMode {
        NOT_PERSISTENT(1),
        PERSISTENT(2);

        private int deliveryMode;

        DeliveryMode(int value) {
            this.deliveryMode = value;
        }

        public int getDeliveryMode() {
            return deliveryMode;
        }
    }

}
