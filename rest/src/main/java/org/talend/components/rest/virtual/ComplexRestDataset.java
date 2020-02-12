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
package org.talend.components.rest.virtual;

import lombok.Data;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.processor.JSonExtractorConfiguration;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Version
@DataSet("Dataset")
@GridLayout({ @GridLayout.Row({ "restConfiguration" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "restConfiguration" }) })
public class ComplexRestDataset implements Serializable {

    @Option
    @Documentation("REST connector configuration")
    private RequestConfig restConfiguration;

    @Option
    @Documentation("Json extractor")
    private JSonExtractorConfiguration jSonExtractorConfiguration = new JSonExtractorConfiguration();

}
