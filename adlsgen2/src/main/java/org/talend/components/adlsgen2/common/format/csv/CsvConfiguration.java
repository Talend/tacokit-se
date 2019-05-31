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
package org.talend.components.adlsgen2.common.format.csv;

import java.io.Serializable;

import org.talend.components.adlsgen2.common.format.FileEncoding;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ //
        @GridLayout.Row({ "recordDelimiter", "fieldDelimiter" }), //
        @GridLayout.Row("header"), //
        @GridLayout.Row("csvSchema"), //
        @GridLayout.Row("fileEncoding"), //
})
@Documentation("CSV Configuration")
public class CsvConfiguration implements Serializable {

    @Option
    @DefaultValue("SEMICOLON")
    @Documentation("CSV Field Delimiter")
    private CsvFieldDelimiter fieldDelimiter;

    @Option
    @DefaultValue("LF")
    @Documentation("Record Delimiter")
    private CsvRecordSeparator recordDelimiter;

    @Option
    @Documentation("Schema")
    private String csvSchema;

    @Option
    @Documentation("Has header line")
    private boolean header;

    @Option
    @Documentation("File Encoding")
    private FileEncoding fileEncoding = FileEncoding.UTF8;

}
