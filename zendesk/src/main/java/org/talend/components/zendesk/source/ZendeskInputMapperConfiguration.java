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
package org.talend.components.zendesk.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.talend.components.zendesk.common.ZendeskDataSet;

import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "queryString" }), @GridLayout.Row({ "fields" }) })
@Documentation("'Input component' configuration.")
public class ZendeskInputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Connection to server.")
    private ZendeskDataSet dataset;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @ActiveIf(target = "dataset/selectionType", value = { "TICKETS" })
    @Documentation("Query string. See Zendesk API documentation. Example: 'status:open created>2012-07-17'.")
    private String queryString;

    public boolean isQueryStringEmpty() {
        return queryString == null || queryString.isEmpty();
    }

}