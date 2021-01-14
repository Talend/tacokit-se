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
package org.talend.components.mongodb.dataset;

import lombok.Data;
import org.talend.components.mongodb.*;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.Collections;
import java.util.List;

@Version(1)
@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "mongodb")
@DataSet("MongoDBReadDataSet")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "collection" }), @GridLayout.Row({ "mode" }),
        @GridLayout.Row({ "query" }) })
// @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "enableExternalSort" }) })
@Documentation("MongoDB DataSet for read only")
public class MongoDBReadDataSet implements BaseDataSet {

    @Option
    @Documentation("Connection")
    private MongoDBDataStore datastore;

    @Option
    @Required
    @Documentation("Collection")
    private String collection;

    @Option
    @Required
    @Documentation("Mode")
    private Mode mode = Mode.JSON;

    // TODO almost impossible to split dataset with source and sink, the common part is almost no meaning as dataset
    /*
     * @Option
     * 
     * @ActiveIf(target = "mode", value = "MAPPING")
     * 
     * @Documentation("Path Mapping")
     * private List<PathMapping> pathMappings = Collections.emptyList();
     */

    /*
     * @Option
     * 
     * @Documentation("Query type")
     * private QueryType queryType = QueryType.FIND;
     */

    @Option
    @Code("json")
    // @ActiveIf(target = "queryType", value = "FIND")
    @Documentation("Query")
    private String query = "{}";

    /*
     * @Option
     * 
     * @Code("json")
     * 
     * @ActiveIf(target = "queryType", value = "FIND")
     * 
     * @Documentation("http://mongodb.github.io/mongo-java-driver/4.0/driver-scala/builders/projections/")
     * private String projection = "{}";
     * 
     * @Option
     * 
     * @ActiveIf(target = "queryType", value = "AGGREGATION")
     * 
     * @Documentation("Aggregation stages")
     * private List<AggregationStage> aggregationStages = Collections.emptyList();
     * 
     * @Option
     * 
     * @Documentation("Sort by")
     * private List<SortBy> sortBy = Collections.emptyList();
     * 
     * @Option
     * 
     * @Documentation("Maximum number of documents to be returned")
     * private int limit = -1;
     * 
     * // TODO readonly and only for user view data
     * 
     * @Option
     * 
     * @Code("json")
     * 
     * @ActiveIf(target = "mode", value = "DOCUMENT")
     * 
     * @Documentation("Sample for document json")
     * private String sample;
     * 
     * @Option
     * 
     * @Documentation("Set read preference")
     * private boolean setReadPreference;
     * 
     * @Option
     * 
     * @ActiveIf(target = "setReadPreference", value = "true")
     * 
     * @Documentation("Read preference")
     * private ReadPreference readPreference = ReadPreference.PRIMARY;
     * 
     * @Option
     * 
     * @ActiveIf(target = "queryType", value = "AGGREGATION")
     * 
     * @Documentation("Enable external sort")
     * private boolean enableExternalSort;
     */
}