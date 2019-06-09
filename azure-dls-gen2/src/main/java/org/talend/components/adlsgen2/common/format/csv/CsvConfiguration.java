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
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ //
        @GridLayout.Row({ "fieldDelimiter", "customFieldDelimiter" }), //
        @GridLayout.Row({ "recordSeparator", "customRecordSeparator" }), //
        @GridLayout.Row({ "escapeCharacter", "textEnclosureCharacter" }), //
        @GridLayout.Row("header"), //
        @GridLayout.Row("csvSchema"), //
        @GridLayout.Row({ "fileEncoding", "customEncoding" }), //

})
@Documentation("CSV Configuration")
public class CsvConfiguration implements Serializable {

    @Option
    @Documentation("Symbol(s) used to separate records")
    private CsvRecordSeparator recordSeparator = CsvRecordSeparator.CRLF;

    @Option
    @ActiveIf(target = "recordSeparator", value = "OTHER")
    @Documentation("Your custom record delimiter")
    private String customRecordSeparator;

    @Option
    @Documentation("Symbol(s) used to separate fields")
    private CsvFieldDelimiter fieldDelimiter = CsvFieldDelimiter.SEMICOLON;

    @Option
    @ActiveIf(target = "fieldDelimiter", value = "OTHER")
    @Documentation("Your custom field delimiter")
    private String customFieldDelimiter;

    @Option
    @Documentation("Text enclosure character")
    private String textEnclosureCharacter;

    @Option
    @Documentation("Escape character")
    private String escapeCharacter;

    @Option
    @Documentation("File Encoding")
    private FileEncoding fileEncoding = FileEncoding.UTF8;

    @Option
    @ActiveIf(target = "encoding", value = "OTHER")
    @Documentation("Your custom file encoding format")
    private String customEncoding;

    @Option
    @Documentation("Schema")
    private String csvSchema;

    @Option
    @Documentation("Has header line")
    private boolean header;

    public char effectiveFieldDelimiter() {
        return CsvFieldDelimiter.OTHER.equals(getFieldDelimiter()) ? getCustomFieldDelimiter().charAt(0)
                : getFieldDelimiter().getDelimiter();
    }

    public String effectiveEncoding() {
        return FileEncoding.OTHER.equals(getFileEncoding()) ? getCustomEncoding() : getFileEncoding().getEncoding();
    }

    public String effectiveRecordSeparator() {
        return CsvRecordSeparator.OTHER.equals(getRecordSeparator()) ? getCustomRecordSeparator()
                : getRecordSeparator().getSeparator();
    }

}
