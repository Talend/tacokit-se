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
package org.talend.components.adlsgen2.output;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.talend.components.adlsgen2.common.format.json.JsonConverter;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.AdlsGen2Service.BlobInformations;
import org.talend.components.adlsgen2.service.I18n;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(value = IconType.FILE_CSV_O)
@Processor(name = "AdlsGen2Output", family = "AdlsGen2")
@Documentation("Azure Data Lake Storage Gen2 Output")
public class AdlsGen2Output implements Serializable {

    @Service
    private final AdlsGen2Service service;

    @Service
    private final I18n i18n;

    private OutputConfiguration configuration;

    private transient long position = 0;

    private transient boolean flushNeeded = Boolean.TRUE;

    private List<Record> records;

    private JsonConverter jsonConverter;

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    JsonBuilderFactory jsonBuilderFactory;

    public AdlsGen2Output(@Option("configuration") final OutputConfiguration configuration, final AdlsGen2Service service,
            final I18n i18n, final RecordBuilderFactory recordBuilderFactory, final JsonBuilderFactory jsonBuilderFactory

    ) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
        //
        records = new ArrayList<>();
        // init converter
        jsonConverter = JsonConverter.of(recordBuilderFactory, jsonBuilderFactory,
                configuration.getDataSet().getJsonConfiguration());
    }

    @PostConstruct
    public void init() {
        BlobInformations blob = service.getBlobInformations(configuration.getDataSet());
        if (configuration.isFailOnExistingBlob() && blob.isExists()) {
            String msg = i18n.cannotOverwriteBlob(blob.name);
            log.error(msg);
            flushNeeded = false;
            throw new RuntimeException(msg);
        }
        // TODO get lease
        position = blob.getContentLength();
        if (configuration.isBlobOverwrite() || !blob.isExists()) {
            service.pathCreate(configuration);
            position = 0;
        }
    }

    @ElementListener
    public void onElement(final Record record) {
        records.add(record);
    }

    @PreDestroy
    public void release() {
        StringBuilder content = new StringBuilder();
        for (Record record : records) {
            switch (configuration.getDataSet().getFormat()) {
            case CSV:
                content.append(toCsvFormat(record));
                break;
            case JSON:
                content.append(jsonConverter.fromRecord(record).toString());
                break;
            case AVRO:
            case PARQUET:
                throw new IllegalStateException("format not implemented");
            }
        }
        records.clear();
        service.pathUpdate(configuration, content.toString(), position);
        position += content.length(); // cumulate length of written records

        if (flushNeeded) {
            service.flushBlob(configuration, position);
        }
        // TODO release lease
    }

    private String[] getStringArrayFromRecord(Record record) {
        List<String> values = new ArrayList<>();
        for (Schema.Entry field : record.getSchema().getEntries()) {
            values.add(record.getString(field.getName()));
        }
        return values.toArray(new String[0]);
    }

    private String toCsvFormat(final Record record) {
        StringWriter writer = new StringWriter();
        char fieldDelimiter = configuration.getDataSet().getCsvConfiguration().effectiveFieldDelimiter();
        char recordDelimiter = configuration.getDataSet().getCsvConfiguration().effectiveRecordSeparator().charAt(0);
        CSVFormat csv = CSVFormat.DEFAULT.withRecordSeparator(recordDelimiter).withDelimiter(fieldDelimiter);
        try {
            CSVPrinter printer = new CSVPrinter(writer, csv);

            printer.printRecord(getStringArrayFromRecord(record));
            return writer.toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
