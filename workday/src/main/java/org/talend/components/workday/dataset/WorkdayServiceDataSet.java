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
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Updatable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@DataSet("WorkdayServiceDataSet")
@GridLayout({ @GridLayout.Row("datastore"), @GridLayout.Row({ "module", "service" }), @GridLayout.Row("parameters") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("datastore") })
@Documentation("")
public class WorkdayServiceDataSet implements QueryHelper, Serializable {

    private static final long serialVersionUID = -9037128911796623682L;

    @Option
    @Documentation("The connection to workday datastore")
    private WorkdayDataStore datastore;

    @Option
    @Proposable(value = "workdayModules")
    @Documentation("module of workday")
    private String module;

    @Option
    @Suggestable(value = "workdayServices", parameters = { "module" })
    @Documentation("service of workday")
    private String service;

    @Data
    @GridLayout({ @GridLayout.Row({ "type", "name", "value" }) })
    public static class Parameter implements Serializable {

        private static final long serialVersionUID = 4222585870348980275L;

        public enum Type {
            Query,
            Path
        }

        @Option
        @Documentation("kind (path or query)")
        private Type type;

        @Option
        @Documentation("name")
        private String name;

        @Option
        @Documentation("value")
        private String value = "";

        public void substitute(StringBuilder brut) {
            String pattern = '{' + this.name.trim() + '}';
            int start = brut.indexOf(pattern);
            if (start >= 0) {
                brut.replace(start, start + pattern.length(), value);
            }
        }
    }

    @Data
    @GridLayout({ @GridLayout.Row("parameters") })
    public static class Parameters implements Serializable {

        private static final long serialVersionUID = -8064443311021065570L;

        @Option
        @Documentation("kind (path or query)")
        private List<WorkdayServiceDataSet.Parameter> parameters = new ArrayList<>();

        public String substitute(StringBuilder brut) {
            if (brut == null) {
                return null;
            }
            if (this.parameters != null) {
                this.parameters.forEach((WorkdayServiceDataSet.Parameter p) -> p.substitute(brut));
            }
            return brut.toString();
        }
    }

    @Option
    @Documentation("service parameters")
    @Updatable(value = "workdayServicesParams", parameters = { "module", "service" })
    private Parameters parameters;

    @Override
    public String getServiceToCall() {
        final StringBuilder toCall = new StringBuilder(service);
        if (parameters != null) {
            this.parameters.substitute(toCall);
        }
        if (toCall.toString().startsWith("/")) {
            toCall.deleteCharAt(0);
        }
        return toCall.toString();
    }

    @Override
    public Map<String, String> extractQueryParam() {
        if (parameters == null || parameters.getParameters() == null) {
            return Collections.emptyMap();
        }
        return parameters.getParameters().stream()
                .filter((Parameter x) -> x.type == Parameter.Type.Query && x.value != null && !x.value.isEmpty())
                .collect(Collectors.toMap(Parameter::getName, Parameter::getValue));
    }
}
