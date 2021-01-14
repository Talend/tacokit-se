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
package org.talend.components.bigquery.service;

import com.google.auth.Credentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.bigquery.avro.AvroConverter;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

@Service
@Slf4j
public class GoogleStorageService {

    public Storage getStorage(Credentials credentials) {
        StorageOptions storageOptions = StorageOptions.newBuilder().setCredentials(credentials).build();
        return new StorageOptions.DefaultStorageFactory().create(storageOptions);
    }

    public DataFileStream<GenericRecord> getDataFileStream(Storage storage, String bucket, String gsBlob) throws IOException {
        Blob blob = storage.get(bucket, gsBlob);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        ReadChannel rc = blob.reader();
        InputStream in = Channels.newInputStream(rc);
        return new DataFileStream(in, datumReader);
    }

    public void deleteBlob(Storage storage, String bucket, String gsBlob) {
        Blob blob = storage.get(bucket, gsBlob);
        if (blob != null) {
            blob.delete();
        }
    }
}
