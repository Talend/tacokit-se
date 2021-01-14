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
package org.talend.components.couchbase.output;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "idFieldName", "partialUpdate" }), //
        @GridLayout.Row({ "useN1QLQuery" }), //
        @GridLayout.Row({ "query" }), //
        @GridLayout.Row({ "queryParams" }), //
        }), @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataSet" }) }) })
@Documentation("Couchbase output configuration")
public class CouchbaseOutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private CouchbaseDataSet dataSet;

    @Option
    @Documentation("Use N1QL query")
    private boolean useN1QLQuery = false;

    @Option
    @ActiveIf(target = "useN1QLQuery", value = "false")
    @Documentation("Field to use as ID")
    private String idFieldName;

    @Option
    @ActiveIf(target = "useN1QLQuery", value = "false")
    @Documentation("Do a partial update of document")
    private boolean partialUpdate;

    @Option
    @TextArea
    @Code("sql")
    @ActiveIf(target = "useN1QLQuery", value = "true")
    @Documentation("The N1QL query.")
    private String query;

    @Option
    @ActiveIf(target = "useN1QLQuery", value = "true")
    @Documentation("N1QL Query Parameters")
    private List<N1QLQueryParameter> queryParams = Collections.emptyList();

}
