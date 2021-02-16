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
package org.talend.components.common.formats.csv;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.common.formats.Encoding;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GridLayout(value = { //
        @GridLayout.Row("recordDelimiter"), @GridLayout.Row("customRecordDelimiter"), //
        @GridLayout.Row("fieldDelimiter"), @GridLayout.Row("customFieldDelimiter"), @GridLayout.Row("textEnclosureCharacter"), //
        @GridLayout.Row("escapeCharacter"), @GridLayout.Row("encoding"), @GridLayout.Row("customEncoding"), //
        @GridLayout.Row("useHeader"), @GridLayout.Row("header") })
@Data
public class CSVFormatOptions implements Serializable {

    private static final long serialVersionUID = -3030939392498621435L;

    @Option
    @Documentation("Symbol(s) used to separate records")
    private CSVRecordDelimiter recordDelimiter = CSVRecordDelimiter.CRLF;

    @Option
    @ActiveIf(target = "recordDelimiter", value = "OTHER")
    @Documentation("Your custom record delimiter")
    private String customRecordDelimiter;

    @Option
    @Documentation("Symbol(s) used to separate fields")
    private CSVFieldDelimiter fieldDelimiter = CSVFieldDelimiter.SEMICOLON;

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
    @Documentation("Encoding")
    private Encoding encoding = Encoding.UTF8;

    @Option
    @ActiveIf(target = "encoding", value = "OTHER")
    @Documentation("Your custom file encoding format")
    private String customEncoding;

    @Option
    @Documentation("Set header size")
    private boolean useHeader;

    @Option
    @ActiveIf(target = "useHeader", value = "true")
    @Documentation("Header size")
    // @Min(-0.0)
    // TODO min doesn't work correctly yet
    private int header = 1;

    public char effectiveFieldDelimiter() {
        return CSVFieldDelimiter.OTHER.equals(getFieldDelimiter()) ? getCustomFieldDelimiter().charAt(0)
                : getFieldDelimiter().getDelimiterValue();
    }

    public String effectiveFileEncoding() {
        if (Encoding.OTHER == getEncoding()) {
            try {
                Charset.forName(customEncoding);
                return getCustomEncoding();
            } catch (Exception e) {
                String msg = String.format("Encoding not supported %s.", getCustomEncoding());
                log.error("[effectiveFileEncoding] {}", msg);
                throw new RuntimeException(msg);
            }
        } else {
            return getEncoding().getEncodingCharsetValue();
        }
    }

    public String effectiveRecordSeparator() {
        return CSVRecordDelimiter.OTHER.equals(getRecordDelimiter()) ? getCustomRecordDelimiter()
                : getRecordDelimiter().getDelimiterValue();
    }
}