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
package org.talend.components.cosmosDB.dataset;

import lombok.Data;
import org.talend.components.cosmosDB.datastore.CosmosDBDataStore;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.ArrayList;
import java.util.List;

@Version(1)
@Data
@DataSet("CosmosDBDataset")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "collectionID" }), @GridLayout.Row({ "documentType" }),
        @GridLayout.Row({ "schema" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "datastore" }) })
@Documentation("cosmosDB DataSet")
public class CosmosDBDataset implements BaseDataSet {

    @Option
    @Documentation("Connection")
    private CosmosDBDataStore datastore;

    @Option
    @Documentation("Schema")
    @Structure(type = Structure.Type.OUT, discoverSchema = "discover")
    private List<String> schema = new ArrayList<>();

    @Option
    @Required
    @Documentation("Collection ID")
    private String collectionID;

    @Option
    @Required
    @Documentation("Document type")
    private DocumentType documentType = DocumentType.JSON;

}