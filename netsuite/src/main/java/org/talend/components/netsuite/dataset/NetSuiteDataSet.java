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
package org.talend.components.netsuite.dataset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet
@AllArgsConstructor
@NoArgsConstructor
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataStore" }),
                @GridLayout.Row({ "enableCustomization" }) }) })
@Documentation("Common properties that are present in Input & Output components")
public class NetSuiteDataSet implements Serializable {

    @Option
    @Documentation("Connection")
    private NetSuiteDataStore dataStore;

    @Option
    @DefaultValue("true")
    @Documentation("Enables or disables operations with custom records, fields, entities, forms.")
    private boolean enableCustomization;

    @Option
    @Suggestable(value = UIActionService.LOAD_RECORD_TYPES, parameters = { "dataStore", "enableCustomization" })
    @Documentation("Record Type to be used")
    private String recordType;
}
