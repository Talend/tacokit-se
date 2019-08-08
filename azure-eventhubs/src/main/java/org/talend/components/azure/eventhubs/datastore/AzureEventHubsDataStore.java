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
package org.talend.components.azure.eventhubs.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;
import lombok.Data;

@Data
@DataStore("AzureEventHubsDataStore")
@Checkable("checkEndpoint")
@GridLayout({ @GridLayout.Row({ "endpoint" }), @GridLayout.Row({ "sasKeyName" }), @GridLayout.Row({ "sasKey" }) })
@Documentation("The connection to the azure eventhubs")
public class AzureEventHubsDataStore implements Serializable {

    @Option
    @Required
    @Documentation("A combination of the namespace name and domain name")
    private String endpoint;

    @Option
    @Required
    @Documentation("Shared Access Signature key name")
    private String sasKeyName;

    @Option
    @Credential
    @Required
    @Documentation("Shared Access Signature key")
    private String sasKey;

}