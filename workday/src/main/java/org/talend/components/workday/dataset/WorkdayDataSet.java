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
package org.talend.components.workday.dataset;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@Data
@DataSet("WorkdayDataSet")
@GridLayout({ @GridLayout.Row("datastore"), @GridLayout.Row("mode"), @GridLayout.Row("raas"), @GridLayout.Row("wql") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("datastore") })
@Documentation("Dataset for workday")
@Slf4j
public class WorkdayDataSet implements Serializable, QueryHelper {

    private static final long serialVersionUID = 5305679660126846088L;

    @Option
    @Documentation("The connection to workday datastore")
    private WorkdayDataStore datastore;

    public enum WorkdayMode {
        WQL("data"),
        RAAS("Report_Entry");

        public final String arrayName;

        WorkdayMode(String arrayName) {
            this.arrayName = arrayName;
        }
    }

    @Option
    @Documentation("Execution mode for workday")
    private WorkdayMode mode = WorkdayMode.WQL;

    @Option
    @Documentation("Layout for report as a service")
    @ActiveIf(target = "mode", value = "RAAS")
    private RAASLayout raas;

    @Option
    @Documentation("Layout for workday query language")
    @ActiveIf(target = "mode", value = "WQL")
    private WQLLayout wql;

    @Override
    public String getServiceToCall() {
        final QueryHelper helper = this.selectedHelper();
        if (helper == null) {
            log.warn("No service to call for mode {}", this.mode);
            return "";
        }
        return helper.getServiceToCall();
    }

    @Override
    public Map<String, String> extractQueryParam() {
        final QueryHelper helper = this.selectedHelper();
        if (helper == null) {
            log.warn("No query param for mode {}", this.mode);
            return Collections.emptyMap();
        }
        return helper.extractQueryParam();
    }

    private QueryHelper selectedHelper() {
        if (this.mode == WorkdayMode.RAAS) {
            return this.raas;
        }
        if (this.mode == WorkdayMode.WQL) {
            return this.wql;
        }
        return null;
    }
}
