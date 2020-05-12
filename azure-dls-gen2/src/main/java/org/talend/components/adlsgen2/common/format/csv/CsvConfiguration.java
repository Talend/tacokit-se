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
package org.talend.components.adlsgen2.common.format.csv;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.adlsgen2.common.format.FileEncoding;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@GridLayout({ //
        @GridLayout.Row({ "fieldDelimiter", "customFieldDelimiter" }), //
        @GridLayout.Row({ "recordSeparator", "customRecordSeparator" }), //
        @GridLayout.Row({ "textEnclosureCharacter", "escapeCharacter" }), //
        @GridLayout.Row("header"), //
        @GridLayout.Row("csvSchema"), //
        @GridLayout.Row({ "fileEncoding", "customFileEncoding" }), //

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
    @ActiveIf(target = "fileEncoding", value = "OTHER")
    @Documentation("Your custom file encoding format")
    private String customFileEncoding;

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

    public String effectiveFileEncoding() {
        if (FileEncoding.OTHER == getFileEncoding()) {
            try {
                Charset.forName(customFileEncoding);
                return getCustomFileEncoding();
            } catch (Exception e) {
                String msg = String.format("Encoding not supported %s.", customFileEncoding);
                log.error("[effectiveFileEncoding] {}", msg);
                throw new AdlsGen2RuntimeException(msg);
            }
        } else {
            return getFileEncoding().getEncoding();
        }
    }

    public String effectiveRecordSeparator() {
        return CsvRecordSeparator.OTHER.equals(getRecordSeparator()) ? getCustomRecordSeparator()
                : getRecordSeparator().getSeparator();
    }

    public List<String> getCsvSchemaHeaders() {
        List<String> headers = new ArrayList<>();
        if (StringUtils.isEmpty(csvSchema)) {
            return headers;
        }
        try {
            return Arrays.stream(csvSchema.split(String.valueOf(effectiveFieldDelimiter()))).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[getCsvSchemaHeaders] Cannot get Headers from {}: {}", csvSchema, e.getMessage());
        }
        return headers;
    }

}
