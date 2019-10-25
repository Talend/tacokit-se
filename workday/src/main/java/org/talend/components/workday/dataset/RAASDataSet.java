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
package org.talend.components.workday.dataset;

import lombok.Data;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@Data
@DataSet("ReportAsAServiceDataset")
@GridLayout({ @GridLayout.Row("datastore"), @GridLayout.Row({ "user", "report" }) })
@Documentation("RAAS dataset for workday (Report As A Service)")
public class RAASDataSet implements Serializable, QueryHelper {

    private static final long serialVersionUID = 5305679660126846088L;

    @Option
    @Documentation("The connection to workday datastore")
    private WorkdayDataStore datastore;

    @Option
    @Documentation("The user who made the report")
    private String user;

    @Option
    @Documentation("report name")
    private String report;

    @Override
    public String getServiceToCall() {
        return "raas/" + this.user + '/' + this.report;
    }

    @Override
    public Map<String, String> extractQueryParam() {
        return Collections.emptyMap();
    }

}
