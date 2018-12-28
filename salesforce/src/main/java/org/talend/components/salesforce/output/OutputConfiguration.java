/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.output;

import java.io.Serializable;

import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "moduleDataSet" }), @GridLayout.Row({ "outputAction" }), @GridLayout.Row({ "upsertKeyColumn" }),
        @GridLayout.Row({ "batchMode" }), @GridLayout.Row("commitLevel"), @GridLayout.Row("exceptionForErrors") })
@Documentation("This configuration of output component")
public class OutputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("the configuration of connection and target module")
    private ModuleDataSet moduleDataSet;

    @Option
    @Required
    @Documentation("write operation")
    private OutputAction outputAction = OutputAction.INSERT;

    @Option
    @ActiveIf(target = "outputAction", value = "UPSERT")
    @Documentation("key column for upsert")
    private String upsertKeyColumn;

    @Option
    @Required
    @Documentation("whether use batch operation")
    private boolean batchMode = true;

    @Option
    @ActiveIf(target = "batchMode", value = "true")
    @Documentation("max size of batch")
    private int commitLevel = 200;

    @Option
    @Required
    @DefaultValue("true")
    @Documentation("whether throw exception when got error during operation")
    private boolean exceptionForErrors;

    public enum OutputAction {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }

}