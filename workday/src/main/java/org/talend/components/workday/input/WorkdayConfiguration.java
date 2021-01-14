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
package org.talend.components.workday.input;

import lombok.Data;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Version(1)
@GridLayout(value = { @GridLayout.Row({ "dataSet" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("dataSet") })
@Documentation("Workday configuration")
public class WorkdayConfiguration implements Serializable {

    private static final long serialVersionUID = 5453557029320067473L;

    @Option
    @Documentation("Dataset")
    private WorkdayDataSet dataSet;
}
