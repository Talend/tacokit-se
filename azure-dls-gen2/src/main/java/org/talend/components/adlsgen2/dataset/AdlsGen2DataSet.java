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
package org.talend.components.adlsgen2.dataset;

import java.io.Serializable;

import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.avro.AvroConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.json.JsonConfiguration;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConfiguration;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.adlsgen2.service.UIActionService.ACTION_FILESYSTEMS;

@Data
@DataSet("AdlsGen2DataSet")
@GridLayout({ //
        @GridLayout.Row("connection"), //
        @GridLayout.Row("filesystem"), //
        @GridLayout.Row("blobPath"), //
        @GridLayout.Row("format"), //
        @GridLayout.Row("csvConfiguration"), //
        @GridLayout.Row("avroConfiguration"), //
        @GridLayout.Row("parquetConfiguration"), //
        @GridLayout.Row("jsonConfiguration"), //
})
@Documentation("ADLS DataSet")
public class AdlsGen2DataSet implements Serializable {

    @Option
    @Required
    @Documentation("ADLS Gen2 Connection")
    private AdlsGen2Connection connection;

    @Option
    @Required
    @Suggestable(value = ACTION_FILESYSTEMS, parameters = { "connection" })
    @Documentation("FileSystem")
    private String filesystem;

    @Option
    @Required
    @Documentation("Path to Blob Object")
    private String blobPath;

    @Option
    @Required
    @DefaultValue("CSV")
    @Documentation("Format of Blob content")
    private FileFormat format;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    private CsvConfiguration csvConfiguration;

    @Option
    @ActiveIf(target = "format", value = "AVRO")
    private AvroConfiguration avroConfiguration;

    @Option
    @ActiveIf(target = "format", value = "PARQUET")
    private ParquetConfiguration parquetConfiguration;

    @Option
    @ActiveIf(target = "format", value = "JSON")
    private JsonConfiguration jsonConfiguration;

}
