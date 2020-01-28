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
package org.talend.components.common.stream.format;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("lineConfiguration"), @GridLayout.Row({ "fieldSeparator" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "escape", "quotedValue" }) })
public class CSVConfiguration implements ContentFormat {

    private static final long serialVersionUID = -6803208558417743486L;

    static {
        try {
            Class.forName("org.talend.components.common.stream.input.csv.CSVReaderSupplier");
            Class.forName("org.talend.components.common.stream.output.csv.CSVWriterSupplier");
        } catch (ClassNotFoundException e) {
            // not exist if no dependencies to stream-csv, csv format is not used.
        }
    }

    @Option
    @Documentation("line delimiter")
    private LineConfiguration lineConfiguration;

    @Option
    @Documentation("field delimiter")
    private Character fieldSeparator;

    @Option
    @Documentation("Escape character")
    private Character escape = '\\';

    @Option
    @Documentation("Text enclosure character")
    private Character quotedValue = '"';

}