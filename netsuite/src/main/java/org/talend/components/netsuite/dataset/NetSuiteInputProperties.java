/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.dataset;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "searchCondition" }) }), @GridLayout(names = {
        GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "bodyFieldsOnly" }) }) })
@Documentation("Properties for Input component")
public class NetSuiteInputProperties implements Serializable {

    @Option
    @Documentation("Common dataset properties - datastore + module")
    private NetSuiteDataSet dataSet;

    @Option
    @Documentation("Properties that are required for search")
    private List<SearchConditionConfiguration> searchCondition;

    /**
     * @param bodyFieldsOnly In terms of documentation false means get all fields, true no item lists returned
     */
    @Option
    @Documentation("Shows or hides Item List result. Default true - hides, uncheck it to show")
    private boolean bodyFieldsOnly = true;
}
