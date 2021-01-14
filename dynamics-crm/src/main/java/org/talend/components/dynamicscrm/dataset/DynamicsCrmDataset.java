/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.dynamicscrm.dataset;

import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

import java.io.Serializable;

import org.talend.components.dynamicscrm.datastore.DynamicsCrmConnection;
import org.talend.components.dynamicscrm.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("DynamicsCrmDataset")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "entitySet" }) })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("datastore") })
@Documentation("Dynamics CRM dataset")
public class DynamicsCrmDataset implements Serializable {

    @Option
    @Documentation("Dynamics CRM connection")
    private DynamicsCrmConnection datastore;

    @Option
    @Required
    @Suggestable(value = UIActionService.ACTION_ENTITY_SETS_DYNAMICS365, parameters = { "datastore" })
    @Documentation("Name of entity set")
    private String entitySet;

}