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
package org.talend.components.adlsgen2.output;

import java.io.Serializable;

import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(value = { //
        @GridLayout.Row({ "dataSet" }), //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { //
        @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "blobNameTemplate" }) })
@Documentation("ADLS output configuration")
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private AdlsGen2DataSet dataSet;

    @Option
    @Documentation("Generated blob item name prefix.\nBatch file would have name prefix + UUID + extension.\n"
            + "I.e. myPrefix-5deaa8ff-7d22-4b86-a864-9a6fa414501a.avro")
    private String blobNameTemplate = "data-";

}
