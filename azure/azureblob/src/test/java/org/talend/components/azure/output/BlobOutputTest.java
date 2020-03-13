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
package org.talend.components.azure.output;

import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import static org.mockito.ArgumentMatchers.any;

public class BlobOutputTest {

    AzureBlobComponentServices blobComponentServicesMock;

    @BeforeEach
    public void initMocks() throws URISyntaxException {
        CloudBlobClient blobClientMock = Mockito.mock(CloudBlobClient.class);
        CloudStorageAccount cloudStorageAccountMock = Mockito.mock(CloudStorageAccount.class);
        AzureComponentServices componentServicesMock = Mockito.mock(AzureComponentServices.class);
        Mockito.when(componentServicesMock.createCloudBlobClient(any(), any())).thenReturn(blobClientMock);
        blobComponentServicesMock = Mockito.mock(AzureBlobComponentServices.class);
        Mockito.when(blobComponentServicesMock.getConnectionService()).thenReturn(componentServicesMock);
        Mockito.when(blobComponentServicesMock.createStorageAccount(any())).thenReturn(cloudStorageAccountMock);
    }

    @Test
    void testHTMLOutputNotSupported() {
        final String expectedExceptionMessage = "HTML excel output is not supported";
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setExcelFormat(ExcelFormat.HTML);

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setDirectory("testDir");
        dataset.setFileFormat(FileFormat.EXCEL);
        dataset.setExcelOptions(excelFormatOptions);
        BlobOutputConfiguration outputConfiguration = new BlobOutputConfiguration();
        outputConfiguration.setDataset(dataset);

        BlobOutput output = new BlobOutput(outputConfiguration, blobComponentServicesMock, Mockito.mock(MessageService.class));
        BlobRuntimeException thrownException = Assertions.assertThrows(BlobRuntimeException.class, output::init);

        Assert.assertEquals("Exception message is different", expectedExceptionMessage, thrownException.getCause().getMessage());
    }

    @Test
    void testCSVOutputWithEmptyDirNotFailing() {
        final String expectedExceptionMessage = "Directory for output action should be specified";
        CSVFormatOptions csvFormatOptions = new CSVFormatOptions();
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setDirectory(null);
        dataset.setFileFormat(FileFormat.CSV);
        dataset.setCsvOptions(csvFormatOptions);
        BlobOutputConfiguration outputConfiguration = new BlobOutputConfiguration();
        outputConfiguration.setDataset(dataset);

        BlobOutput output = new BlobOutput(outputConfiguration, blobComponentServicesMock, Mockito.mock(MessageService.class));
        Assertions.assertDoesNotThrow(output::init);
    }
}