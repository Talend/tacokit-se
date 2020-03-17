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
package org.talend.components.cosmosDB.output;

import java.io.Serializable;

import org.talend.components.cosmosDB.dataset.CosmosDBDataset;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.cosmosDB.service.CosmosDBService.ACTION_SUGGESTION_TABLE_COLUMNS_NAMES;

@Version(1)
@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataset" }), //
        @GridLayout.Row({ "createCollection" }), //
        @GridLayout.Row({ "dataAction" }), //
        @GridLayout.Row({ "autoIDGeneration" }), //
        @GridLayout.Row({ "idFieldName" }), //
        }), @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }),
                @GridLayout.Row({ "offerThroughput" }) }) })
@Documentation("cosmosDB output configuration")
public class CosmosDBOutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private CosmosDBDataset dataset;

    @Option
    @Documentation("Data Action")
    private DataAction dataAction = DataAction.INSERT;

    @Option
    @Documentation("Create collection if not exist")
    private boolean createCollection;

    @Option
    @ActiveIf(target = "createCollection", value = "true")
    @Documentation("Collection Offer Throughput")
    private int offerThroughput = 400;

    @Option
    @Documentation("Auto generation ID")
    private boolean autoIDGeneration;

    @Option
    @ActiveIf(target = "autoIDGeneration", value = "false")
    @Suggestable(value = ACTION_SUGGESTION_TABLE_COLUMNS_NAMES, parameters = { "dataset/schema" })
    @Documentation("ID field")
    private String idFieldName;

}
