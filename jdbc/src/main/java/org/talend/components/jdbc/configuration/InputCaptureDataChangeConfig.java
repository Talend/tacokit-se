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
package org.talend.components.jdbc.configuration;

import lombok.Data;
import org.talend.components.jdbc.dataset.ChangeDataCaptureDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row("changeOffsetOnRead") })
@Documentation("Stream table input configuration")
public class InputCaptureDataChangeConfig implements InputConfig {

    @Option
    @Documentation("stream table name dataset")
    private ChangeDataCaptureDataset dataSet;

    @Option
    @Documentation("Change offset on read")
    private ChangeOffsetOnReadStrategy changeOffsetOnRead = ChangeOffsetOnReadStrategy.NO;

    public enum ChangeOffsetOnReadStrategy {
        YES,
        NO
    }

}
