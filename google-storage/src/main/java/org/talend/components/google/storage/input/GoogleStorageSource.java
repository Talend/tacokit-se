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
package org.talend.components.google.storage.input;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.talend.components.common.collections.IteratorComposer;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.StorageFacade;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Version
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "cloudstorage")
@Emitter(family = "GoogleStorage", name = "Input")
@Documentation("This component read content file from google cloud storage.")
@RequiredArgsConstructor
public class GoogleStorageSource implements Serializable {

    private static final long serialVersionUID = 7373818898514942128L;

    /** google storage input configuration. */
    private final InputConfiguration config;

    /** record factory */
    private final RecordBuilderFactory factory;

    private final RecordIORepository ioRepository;

    private final GSService services;

    /** current record iterator */
    private transient Iterator<Record> recordIterator = null;

    @Producer
    public Record next() {
        if (recordIterator == null) {
            this.recordIterator = this.buildRecordIterator();
        }
        if (this.recordIterator != null && this.recordIterator.hasNext()) {
            return recordIterator.next();
        }
        return null;
    }

    private Iterator<Record> buildRecordIterator() {
        // blob name list
        final GSDataSet dataset = this.getDataSet();
        final StorageFacade storage = this.services.buildStorage(dataset.getDataStore().getJsonCredentials());
        this.services.checkBucket(storage, dataset.getBucket());
        this.services.checkBlob(storage, dataset.getBucket(), dataset.getBlob());
        final Stream<String> blobsName = storage.findBlobsName(dataset.getBucket(), dataset.getBlob());

        // reader depending on format.
        final RecordReader recordReader = this.buildReader();

        // build iterator on record for each input
        return IteratorComposer.of(blobsName.iterator()) //
                .map((String name) -> storage.buildInput(dataset.getBucket(), name)) // create input stream
                .map((Supplier<InputStream> input) -> new RecordsInputStream(recordReader, input)) //
                .flatmap(RecordsInputStream::records) //
                .build();
    }

    private RecordReader buildReader() {
        // reader depending on format.
        final ContentFormat format = this.getDataSet().getContentFormat().findFormat();
        final RecordReaderSupplier recordReaderSupplier = this.ioRepository.findReader(format.getClass());
        return recordReaderSupplier.getReader(this.factory, format);
    }

    private GSDataSet getDataSet() {
        return this.config.getDataset();
    }
}
