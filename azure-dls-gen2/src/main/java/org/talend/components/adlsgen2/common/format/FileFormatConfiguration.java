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
package org.talend.components.adlsgen2.common.format;

import java.io.Serializable;

import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
public class FileFormatConfiguration implements Serializable {

    @Option
    @Required
    @DefaultValue("CSV")
    @Documentation("File Format")
    private FileFormat fileFormat;

    @Option
    @ActiveIf(target = "fileFormat", value = "CSV")
    @Documentation("CSV Configuration")
    private CsvConfiguration csvConfiguration;

    @Option
    @ActiveIf(target = "fileFormat", value = "AVRO")
    @Documentation("AVRO Configuration")
    private CsvConfiguration avroConfiguration;

    @Option
    @ActiveIf(target = "fileFormat", value = "JSON")
    @Documentation("JSON Configuration")
    private CsvConfiguration jsonConfiguration;

    @Option
    @ActiveIf(target = "fileFormat", value = "PARQUET")
    @Documentation("Parquet Configuration")
    private CsvConfiguration parquetConfiguration;

}
