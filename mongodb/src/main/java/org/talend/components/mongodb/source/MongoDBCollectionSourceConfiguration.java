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
package org.talend.components.mongodb.source;

import lombok.Data;
import org.talend.components.mongodb.dataset.MongoDBReadAndWriteDataSet;
import org.talend.components.mongodb.dataset.MongoDBReadDataSet;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

@Version(1)
@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataset" }) }),
        @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }) }) })
@Documentation("Simple MongoDB Source Configuration which not support query")
public class MongoDBCollectionSourceConfiguration implements BaseSourceConfiguration {

    @Option
    @Documentation("dataset")
    private MongoDBReadAndWriteDataSet dataset;

    private Long sampleLimit = -1L;

}