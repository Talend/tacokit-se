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
package org.talend.components.google.storage.dataset;

import java.io.Serializable;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.components.common.stream.format.fixed.FixedConfiguration;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@GridLayout({ @GridLayout.Row("contentFormat"), //
        @GridLayout.Row({ "csvConfiguration", "avroConfiguration", "excelConfiguration" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "csvConfiguration" }) })
@Documentation("Stream content configuration.")
public class FormatConfiguration implements Serializable {

    private static final long serialVersionUID = -4143993987459031885L;

    public enum Type {
        CSV,
        AVRO,
        EXCEL
    }

    @Option
    @Documentation("Type of stream content format.")
    private FormatConfiguration.Type contentFormat;

    @Option
    @ActiveIf(target = "contentFormat", value = "CSV")
    @Documentation("CSV format.")
    private CSVConfiguration csvConfiguration;

    @Option
    @ActiveIf(target = "contentFormat", value = "AVRO")
    @Documentation("Avro format.")
    private AvroConfiguration avroConfiguration;

    @Option
    @ActiveIf(target = "contentFormat", value = "EXCEL")
    @Documentation("Excel format.")
    private ExcelConfiguration excelConfiguration;

    public ContentFormat findFormat() {
        if (this.contentFormat == FormatConfiguration.Type.CSV) {
            return this.csvConfiguration;
        }
        if (this.contentFormat == FormatConfiguration.Type.AVRO) {
            return this.avroConfiguration;
        }
        if (this.contentFormat == FormatConfiguration.Type.EXCEL) {
            return this.excelConfiguration;
        }
        throw new IllegalArgumentException(
                "Wrong value for contentFormat : " + (contentFormat == null ? "null" : this.contentFormat.name()));
    }
}
