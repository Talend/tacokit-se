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

package org.talend.components.mongodb.source;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import org.talend.components.mongodb.dataset.MongoDBDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayouts({
        @GridLayout(value = { @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "setReadPreference" }),
                @GridLayout.Row({ "readPreference" }), @GridLayout.Row({ "queryType" }), @GridLayout.Row({ "aggregationStages" }),
                @GridLayout.Row({ "query" }), @GridLayout.Row({ "limit" }), @GridLayout.Row({ "mapping" }),
                @GridLayout.Row({ "sort" }) }, names = GridLayout.FormType.MAIN),
        @GridLayout(value = { @GridLayout.Row({ "noQueryTimeout" }),
                @GridLayout.Row({ "externalSort" }) }, names = GridLayout.FormType.ADVANCED), })
@Documentation("Configuration for MongoDB input component")
public class MongoDBInputMapperConfiguration implements Serializable {

    @Option
    @Documentation("Dataset to use for MongoDB input component")
    private MongoDBDataset dataset;

    @Option
    @Documentation("Set read preference")
    private boolean setReadPreference;

    @Option
    @ActiveIf(target = "setReadPreference", value = "true")
    @Documentation("Read preference type")
    private ReadPreference readPreference;

    public enum ReadPreference {
        PRIMARY,
        PRIMARY_PREFERRED,
        SECONDARY,
        SECONDARY_PREFERRED,
        NEAREST;
    }

    public enum QueryType {
        FIND_QUERY,
        AGGREGATION_PIPELINE_QUERY;
    }

    @Option
    @Documentation("Query type to use to get the data")
    @DefaultValue("FIND_QUERY")
    private QueryType queryType;

    @Option
    @TextArea
    @Documentation("Query to use to get the data")
    @ActiveIf(target = "queryType", value = "FIND_QUERY")
    private String query = "{}";

    @Option
    @Documentation("Limit")
    @ActiveIf(target = "queryType", value = "FIND_QUERY")
    private int limit;

    @Option
    @Documentation("Columns to documents fields mapping")
    private List<InputMapping> mapping;

    @Option
    @Documentation("Columns to sort data by")
    @ActiveIf(target = "queryType", value = "FIND_QUERY")
    private List<Sort> sort;

    @Option
    @Documentation("Set no timeout for queries")
    @ActiveIf(target = "queryType", value = "FIND_QUERY")
    private boolean noQueryTimeout;

    @Option
    @Documentation("Aggregation stages")
    @ActiveIf(target = "queryType", value = "AGGREGATION_PIPELINE_QUERY")
    private List<AggregationStage> aggregationStages;

    @Option
    @Documentation("Enable external sort")
    @ActiveIf(target = "queryType", value = "AGGREGATION_PIPELINE_QUERY")
    private boolean externalSort;

}