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
package org.talend.components.azure;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.runtime.converters.CSVConverter;
import org.talend.components.azure.source.BlobInputProperties;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class BlobTestUtils {

    public static RecordBuilderFactory recordBuilderFactory;

    public static AzureCloudConnection createCloudConnection() {
        Server account;
        final MavenDecrypter decrypter = new MavenDecrypter();

        AzureCloudConnection dataStore = new AzureCloudConnection();
        dataStore.setUseAzureSharedSignature(false);
        AzureStorageConnectionAccount accountConnection = new AzureStorageConnectionAccount();
        account = decrypter.find("azure.account");
        accountConnection.setAccountName(account.getUsername());
        accountConnection.setAccountKey(account.getPassword());

        dataStore.setAccountConnection(accountConnection);
        return dataStore;
    }

    public static void createStorage(String storageName, CloudStorageAccount connectionAccount)
            throws URISyntaxException, StorageException {
        CloudBlobClient blobConnection = connectionAccount.createCloudBlobClient();
        blobConnection.getContainerReference(storageName).createIfNotExists();
    }

    public static void uploadTestFile(CloudStorageAccount storageAccount, BlobInputProperties blobInputProperties,
            String resourceName, String targetName) throws URISyntaxException, StorageException, IOException {
        CloudBlobContainer container = storageAccount.createCloudBlobClient()
                .getContainerReference(blobInputProperties.getDataset().getContainerName());
        CloudBlockBlob blockBlob = container
                .getBlockBlobReference(blobInputProperties.getDataset().getDirectory() + "/" + targetName);

        File resourceFile = new File(BlobTestUtils.class.getClassLoader().getResource(resourceName).toURI());
        try (FileInputStream fileInputStream = new FileInputStream(resourceFile)) {
            blockBlob.upload(fileInputStream, resourceFile.length());
        }
    }

    public static void createAndPopulateFileInStorage(CloudStorageAccount connectionAccount, AzureBlobDataset fileOptions,
            List<String> recordSchema, int recordsSize) throws Exception {
        String fileName = "file" + RandomStringUtils.randomAlphabetic(5);
        String directory = fileOptions.getDirectory();
        if (directory != null) {
            fileName = directory + "/" + fileName;
        }
        CloudBlockBlob file = connectionAccount.createCloudBlobClient().getContainerReference(fileOptions.getContainerName())
                .getBlockBlobReference(fileName);
        byte[] content;
        if (fileOptions.getFileFormat() == FileFormat.CSV) {
            content = createCSVFileContent(recordsSize, recordSchema, fileOptions.getCsvOptions());
        } else {
            throw new IllegalStateException("Only CSV file format supported in this method");
        }
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content)) {
            file.upload(inputStream, content.length);
        }
    }

    private static byte[] createCSVFileContent(int recordsSize, List<String> columns, CSVFormatOptions formatOptions)
            throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));
        CSVFormat format = CSVConverter.of(recordBuilderFactory, formatOptions).getCsvFormat();

        CSVPrinter printer = new CSVPrinter(writer, format);

        for (int i = 0; i < recordsSize; i++) {
            List<String> values = new ArrayList<>();
            columns.forEach(column -> values.add(RandomStringUtils.randomAlphabetic(5)));

            printer.printRecord(values.toArray());
        }
        printer.flush();
        printer.close();
        return byteArrayOutputStream.toByteArray();
    }

    public static List<Record> readDataFromCSVDirectory(String directoryName, CloudStorageAccount connectionAccount,
            AzureBlobDataset config, CSVFormat format) throws URISyntaxException, StorageException, IOException {
        List<CSVRecord> csvRecords = readCSVRecords(directoryName, connectionAccount, config, format);
        CSVConverter converter = CSVConverter.of(recordBuilderFactory, config.getCsvOptions());
        return csvRecords.stream().map(converter::toRecord).collect(Collectors.toList());
    }

    private static List<CSVRecord> readCSVRecords(String directoryName, CloudStorageAccount connectionAccount,
            AzureBlobDataset config, CSVFormat format) throws URISyntaxException, StorageException {
        List<CSVRecord> records = new ArrayList<>();
        CloudBlobDirectory directory = connectionAccount.createCloudBlobClient().getContainerReference(config.getContainerName())
                .getDirectoryReference(directoryName);

        Iterator<ListBlobItem> blobs = directory.listBlobs().iterator();
        while (blobs.hasNext()) {
            try (InputStreamReader reader = new InputStreamReader(((CloudBlob) blobs.next()).openInputStream())) {
                CSVParser parser = new CSVParser(reader, format);
                records.addAll(parser.getRecords());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
        return records;
    }

    public static void deleteStorage(String storageName, CloudStorageAccount connectionAccount)
            throws URISyntaxException, StorageException {
        CloudBlobClient blobConnection = connectionAccount.createCloudBlobClient();
        blobConnection.getContainerReference(storageName).deleteIfExists();
    }

    public static List<Record> fillTestRecords(int recordSize) {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            Record testRecord = recordBuilderFactory.newRecordBuilder().withString("stringValue", "abc" + i).build();
            testRecords.add(testRecord);
        }
        return testRecords;
    }
}
