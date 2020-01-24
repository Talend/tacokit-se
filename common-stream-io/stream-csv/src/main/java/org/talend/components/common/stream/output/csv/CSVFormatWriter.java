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
package org.talend.components.common.stream.output.csv;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.talend.components.common.stream.CSVHelper;
import org.talend.components.common.stream.api.output.FormatWriter;
import org.talend.components.common.stream.format.CSVConfiguration;
import org.talend.sdk.component.api.record.Record;

public class CSVFormatWriter implements FormatWriter<CSVConfiguration> {

    @Override
    public byte[] start(CSVConfiguration config, Record first) {
        final CSVFormat csvFormat = CSVHelper.getCsvFormat(config);
        String[] headers = csvFormat.getHeader();

        if (headers != null && headers.length > 0) {
            final String header = Arrays.asList(headers).stream().collect(Collectors.joining(config.getFieldSeparator() + ""));
            return header.getBytes(Charset.defaultCharset());
        }
        return null;
    }

    @Override
    public byte[] between(CSVConfiguration config) {
        return config.getLineConfiguration().getLineSeparator().getBytes();
    }

    @Override
    public byte[] end(CSVConfiguration config) {
        return null;
    }
}
