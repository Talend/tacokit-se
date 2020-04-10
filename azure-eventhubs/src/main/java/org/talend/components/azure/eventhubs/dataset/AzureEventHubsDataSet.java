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
package org.talend.components.azure.eventhubs.dataset;

import java.io.Serializable;

import org.talend.components.azure.eventhubs.datastore.AzureEventHubsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("AzureEventHubsDataSet")
@GridLayout({ @GridLayout.Row({ "connection" }), @GridLayout.Row({ "eventHubName" }) })
@Documentation("The dataset consume message in eventhubs")
public class AzureEventHubsDataSet implements Serializable {

    @Option
    @Documentation("Connection information to eventhubs")
    private AzureEventHubsDataStore connection;

    @Option
    @Required
    @Validable(value = "checkEventHub", parameters = { "connection", "." })
    @Documentation("The name of the event hub connect to")
    private String eventHubName;

}