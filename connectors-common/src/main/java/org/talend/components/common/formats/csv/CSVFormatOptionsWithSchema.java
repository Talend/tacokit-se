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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GridLayout(value = { //
        @GridLayout.Row("csvFormatOptions"), @GridLayout.Row("csvSchema") })
@Data
@Documentation("Basic CSV configuration and CSV Schema")
public class CSVFormatOptionsWithSchema implements Serializable {

    private static final long serialVersionUID = 2210924710439683018L;

    @Option
    @Documentation("Basic CSV Format options")
    private CSVFormatOptions csvFormatOptions;

    @Option
    @Documentation("Schema")
    private String csvSchema;

    public List<String> getCsvSchemaHeaders() {
        List<String> headers = new ArrayList<>();
        if (StringUtils.isEmpty(csvSchema)) {
            return headers;
        }
        try {
            return Arrays.stream(csvSchema.split(String.valueOf(csvFormatOptions.effectiveFieldDelimiter())))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("[getCsvSchemaHeaders] Cannot get Headers from {}: {}", csvSchema, e.getMessage());
        }
        return headers;
    }
}