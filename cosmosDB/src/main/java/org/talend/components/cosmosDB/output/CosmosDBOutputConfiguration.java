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
package org.talend.components.cosmosDB.output;

import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.AND;

import java.io.Serializable;

import org.talend.components.cosmosDB.dataset.QueryDataset;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(2)
@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataset" }), //
        @GridLayout.Row({ "createCollection" }), //
        @GridLayout.Row({ "dataAction" }), //
        @GridLayout.Row({ "autoIDGeneration" }), //
        }),
        @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }),
                @GridLayout.Row({ "offerThroughput" }), @GridLayout.Row({ "partitionKey" }),
                @GridLayout.Row({ "partitionKeyForDelete" }) }) })
@Documentation("cosmosDB output configuration")
public class CosmosDBOutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private QueryDataset dataset;

    @Option
    @Documentation("Data Action")
    private DataAction dataAction = DataAction.INSERT;

    @Option
    @ActiveIf(target = "dataAction", value = { "INSERT", "UPSERT" })
    @Documentation("Create collection if not exist")
    private boolean createCollection;

    @Option
    @ActiveIfs(operator = AND, value = { @ActiveIf(target = "dataAction", value = { "INSERT", "UPSERT" }),
            @ActiveIf(target = "createCollection", value = "true") })
    @Documentation("Collection Offer Throughput")
    private int offerThroughput = 400;

    @Option
    @Documentation("Partition Key ")
    @ActiveIfs(operator = AND, value = { @ActiveIf(target = "dataAction", value = { "INSERT", "UPSERT" }),
            @ActiveIf(target = "createCollection", value = "true") })
    private String partitionKey;

    @Option
    @Documentation("Partition Key ")
    @ActiveIf(target = "dataAction", value = "DELETE")
    private String partitionKeyForDelete;

    @Option
    @Documentation("Auto generation ID")
    @ActiveIf(target = "dataAction", value = { "INSERT", "UPSERT" })
    private boolean autoIDGeneration;

}
