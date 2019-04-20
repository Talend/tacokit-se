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

package org.talend.components.mongodb.output;

import java.io.Serializable;
import java.util.List;

import lombok.Data;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.service.UIMongoDBService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayouts({
        @GridLayout(value = { @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "setWriteConcern" }),
                @GridLayout.Row({ "writeConcern" }), /* @GridLayout.Row({ "dropCollectionIfExists" }), */
                @GridLayout.Row({ "actionOnData", "updateAllDocuments" }), @GridLayout.Row({ "outputMappingList" }),
                @GridLayout.Row({ "keys" }) }, names = GridLayout.FormType.MAIN),
        @GridLayout(value = { @GridLayout.Row({ "bulkWrite" }),
                @GridLayout.Row({ "bulkWriteType" }) }, names = GridLayout.FormType.ADVANCED) })
@Documentation("Configuration class for output component")
public class MongoDBOutputConfiguration implements Serializable {

    public enum WriteConcern {
        ACKNOWLEDGED,
        UNACKNOWLEDGED,
        JOURNALED,
        REPLICA_ACKNOWLEDGED;
    }

    public enum BulkWriteType {
        ORDERED,
        UNORDERED;
    }

    public enum ActionOnData {
        INSERT,
        UPDATE,
        SET,
        UPSERT,
        UPSERT_WITH_SET,
        DELETE;
    }

    @Option
    @Documentation("Dataset")
    private MongoDBDataset dataset;

    @Option
    @Documentation("Set write concern")
    private boolean setWriteConcern;

    @Option
    @Documentation("Write concern")
    @ActiveIf(target = "setWriteConcern", value = "true")
    @DefaultValue("ACKNOWLEDGED")
    private WriteConcern writeConcern;

    @Option
    @Documentation("Set bulk write")
    private boolean bulkWrite;

    @Option
    @Documentation("Bulk write type")
    @ActiveIf(target = "bulkWrite", value = "true")
    @DefaultValue("ORDERED")
    private BulkWriteType bulkWriteType;

    // @Option
    // @Documentation("Drop collection if exists")
    // Not supported at the moment. We don't have possibility to drop the collection only once.
    // We need some method which will be processed once before all processors are starting their work.
    private boolean dropCollectionIfExists = false;

    @Option
    @Documentation("Action on data")
    @DefaultValue("INSERT")
    private ActionOnData actionOnData;

    @Option
    @Documentation("Mapping for output data")
    private List<OutputMapping> outputMappingList;

    @Option
    @Documentation("Keys used for update operations")
    @Suggestable(value = UIMongoDBService.GET_SCHEMA_FIELDS, parameters = { "../dataset" })
    @ActiveIf(target = "actionOnData", value = { "UPDATE", "SET", "UPSERT", "UPSERT_WITH_SET", "DELETE" })
    private List<String> keys;

    @Option
    @Documentation("Update all documents")
    @ActiveIfs({ @ActiveIf(target = "actionOnData", value = { "SET", "UPSERT_WITH_SET" }),
            @ActiveIf(target = "bulkWrite", value = "false") })
    private boolean updateAllDocuments;
}