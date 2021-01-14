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
package org.talend.components.couchbase.source;

import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import lombok.Data;

@Data
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row("selectAction"), @GridLayout.Row("documentId"),
                @GridLayout.Row("query") }),
        @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataSet" }),
                @GridLayout.Row({ "limit" }) }) })

@Documentation("Couchbase input Mapper Configuration")
public class CouchbaseInputConfiguration implements Serializable {

    @Option
    @Documentation("dataset")
    private CouchbaseDataSet dataSet;

    @Option
    @Documentation("Select action")
    @DefaultValue(value = "ALL")
    private SelectAction selectAction = SelectAction.ALL;

    @Option
    @TextArea
    @Code("sql")
    @Documentation("The N1QL query.")
    @ActiveIf(target = "selectAction", value = "N1QL")
    private String query;

    @Option
    @Documentation("Document Id.")
    @ActiveIf(target = "selectAction", value = "ONE")
    private String documentId;

    @Option
    @Documentation("Maximum number of documents to be returned")
    @ActiveIf(target = "selectAction", value = "ALL")
    private String limit = "";

    public CouchbaseDataSet getDataSet() {
        return dataSet;
    }

    public CouchbaseInputConfiguration setDataSet(CouchbaseDataSet dataSet) {
        this.dataSet = dataSet;
        return this;
    }
}