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
package org.talend.components.azure.dataset;

import java.io.Serializable;

import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("containerName"), @GridLayout.Row("directory"),
        @GridLayout.Row("fileFormat"), @GridLayout.Row("csvOptions"), @GridLayout.Row("excelOptions") })
@Data
@DataSet("AzureDataSet")
public class AzureBlobDataset implements Serializable {

    @Option
    @Required
    @Documentation("Azure Connection")
    private AzureCloudConnection connection;

    @Option
    @Documentation("The name of the container to access")
    @Required
    @Suggestable(value = AzureBlobComponentServices.GET_CONTAINER_NAMES, parameters = "connection")
    private String containerName;

    @Option
    @Documentation("The full path of folder in the selected container")
    private String directory;

    @Option
    @Required
    @Documentation("File format")
    private FileFormat fileFormat;

    @Option
    @ActiveIf(target = "fileFormat", value = "CSV")
    @Documentation("CSV format")
    private CSVFormatOptions csvOptions;

    @Option
    @ActiveIf(target = "fileFormat", value = "EXCEL")
    @Documentation("Excel format")
    private ExcelFormatOptions excelOptions;

}