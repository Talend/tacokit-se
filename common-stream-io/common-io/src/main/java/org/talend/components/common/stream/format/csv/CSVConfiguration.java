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
package org.talend.components.common.stream.format.csv;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("lineConfiguration"), @GridLayout.Row({ "fieldSeparator" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "escape", "quotedValue" }) })
public class CSVConfiguration implements ContentFormat {

    private static final long serialVersionUID = -6803208558417743486L;

    @Option
    @Documentation("Line delimiter.")
    private LineConfiguration lineConfiguration;

    @Option
    @Documentation("Field delimiter.")
    private FieldSeparator fieldSeparator;

    @Option
    @Documentation("Escape character.")
    private Character escape = '\\';

    @Option
    @Documentation("Text enclosure character.")
    private Character quotedValue = '"';

    public Character findFieldSeparator() {
        if (this.fieldSeparator == null) {
            return ';';
        }
        return this.fieldSeparator.findFieldSeparator();
    }
}
