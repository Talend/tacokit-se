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
package org.talend.components.adlsgen2.output;

import java.io.Serializable;

import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(value = { //
        @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "actionOnOutput", "overwrite" }), //
})
@Documentation("ADLS output configuration")
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset")
    private org.talend.components.adlsgen2.dataset.AdlsGen2DataSet dataSet;

    @Option
    @ActiveIf(target = "./dataSet/format", value = "CSV")
    @Documentation("Action on output")
    private ActionOnOutput actionOnOutput = ActionOnOutput.DEFAULT;

    @Option
    @ActiveIf(negate = true, target = "./dataSet/format", value = "CSV")
    @Documentation("Overwrite")
    private boolean overwrite = Boolean.FALSE;

    public enum ActionOnOutput {
        DEFAULT,
        APPEND,
        OVERWRITE
    }

    public boolean isFailOnExistingBlob() {
        if (dataSet.getFormat().equals(FileFormat.CSV)) {
            return ActionOnOutput.DEFAULT.equals(actionOnOutput);
        } else {
            return !overwrite;
        }
    }

    public boolean isBlobOverwrite() {
        if (dataSet.getFormat().equals(FileFormat.CSV)) {
            return ActionOnOutput.OVERWRITE.equals(actionOnOutput);
        } else {
            return overwrite;
        }
    }

}
