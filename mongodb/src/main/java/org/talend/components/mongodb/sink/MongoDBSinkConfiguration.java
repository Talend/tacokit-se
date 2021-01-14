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
package org.talend.components.mongodb.sink;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.talend.components.mongodb.BulkWriteType;
import org.talend.components.mongodb.DataAction;
import org.talend.components.mongodb.KeyMapping;
import org.talend.components.mongodb.WriteConcern;
import org.talend.components.mongodb.dataset.MongoDBReadAndWriteDataSet;
import org.talend.components.mongodb.dataset.MongoDBReadDataSet;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataset" }), //
        @GridLayout.Row({ "setWriteConcern" }), //
        @GridLayout.Row({ "writeConcern" }), //
        @GridLayout.Row({ "bulkWrite" }), @GridLayout.Row({ "bulkWriteType" }), @GridLayout.Row({ "dataAction" }),
        @GridLayout.Row({ "keyMappings" }), @GridLayout.Row({ "updateAllDocuments" }) }),
        @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }) }) })
@Documentation("MongoDB sink configuration")
public class MongoDBSinkConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private MongoDBReadAndWriteDataSet dataset;

    @Option
    @Documentation("Set read preference")
    private boolean setWriteConcern;

    @Option
    @ActiveIf(target = "setWriteConcern", value = "true")
    @Documentation("Write concern")
    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

    @Option
    @Documentation("Bulk load")
    private boolean bulkWrite = false;

    @Option
    @ActiveIf(target = "bulkWrite", value = "true")
    @Documentation("Bulk load type")
    private BulkWriteType bulkWriteType = BulkWriteType.UNORDERED;

    @Option
    @Documentation("Data action")
    private DataAction dataAction = DataAction.INSERT;

    @Option
    @ActiveIf(target = "dataAction", value = { "SET", "UPSERT_WITH_SET" })
    @Documentation("Key mappings")
    private List<KeyMapping> keyMappings = Collections.emptyList();

    @Option
    @ActiveIf(target = "dataAction", value = { "SET", "UPSERT_WITH_SET" })
    @Documentation("update all documents")
    private boolean updateAllDocuments = false;

    // this one for work for mapping mode, but seems not necessary now, only whole document(text) and json mode now
    /*
     * @Option
     * 
     * @Documentation("not generate key without value in json if null")
     * private boolean skipNullValue = false;
     */
}
