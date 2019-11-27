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
package org.talend.components.magentocms.output;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.http.HttpException;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.Serializable;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "magento_output")
@Processor(name = "Output")
@Documentation("Data output processor")
public class MagentoOutput implements Serializable {

    private final MagentoOutputConfiguration configuration;

    private final JsonBuilderFactory jsonBuilderFactory;

    private final RecordBuilderFactory recordBuilderFactory;

    private MagentoHttpClientService magentoHttpClientService;

    private MagentoCmsService magentoCmsService;

    // private List<JsonObject> batchData = new ArrayList<>();

    public MagentoOutput(@Option("configuration") final MagentoOutputConfiguration configuration,
            final MagentoHttpClientService magentoHttpClientService, final JsonBuilderFactory jsonBuilderFactory,
            RecordBuilderFactory recordBuilderFactory, MagentoCmsService magentoCmsService) {
        this.configuration = configuration;
        this.magentoHttpClientService = magentoHttpClientService;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.recordBuilderFactory = recordBuilderFactory;
        this.magentoCmsService = magentoCmsService;
        ConfigurationHelper.setupServicesOutput(configuration, magentoHttpClientService);
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<Record> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws UnknownAuthenticationTypeException, IOException {
        processOutputElement(record, success, reject);
    }

    private void addItem(Record.Builder recordBuilder, Schema.Entry schemaEntry, Record record) {
        String schemaName = schemaEntry.getName();
        if (record.get(schemaEntry.getType().getClass(), schemaName) == null) {
            return;
        }
        switch (schemaEntry.getType()) {
        case RECORD:
        case ARRAY:
        case STRING:
        case DATETIME:
            recordBuilder.withString(schemaName, record.get(schemaEntry.getType().getClass(), schemaName).toString());
            break;
        case BYTES:
            recordBuilder.withBytes(schemaName, record.getBytes(schemaName));
            break;
        case INT:
            recordBuilder.withInt(schemaName, record.getInt(schemaName));
            break;
        case LONG:
            recordBuilder.withLong(schemaName, record.getLong(schemaName));
            break;
        case FLOAT:
            recordBuilder.withFloat(schemaName, record.getFloat(schemaName));
            break;
        case DOUBLE:
            recordBuilder.withDouble(schemaName, record.getDouble(schemaName));
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(schemaName, record.getBoolean(schemaName));
            break;
        default:
            recordBuilder.withString(schemaName, record.get(schemaEntry.getType().getClass(), schemaName).toString());
        }
    }

    private void processOutputElement(final JsonObject initialObject, OutputEmitter<Record> success, OutputEmitter<Reject> reject)
            throws UnknownAuthenticationTypeException, IOException {
        Record record = magentoCmsService.jsonObjectToRecord(initialObject, configuration.getMagentoDataSet().getSelectionType());

        try {
            // JsonObject initialObject = magentoCmsService.recordToJsonObject(record);

            // delete 'id'
            final JsonObject copy = initialObject.entrySet().stream().filter(e -> !e.getKey().equals("id"))
                    .collect(jsonBuilderFactory::createObjectBuilder, (builder, a) -> builder.add(a.getKey(), a.getValue()),
                            JsonObjectBuilder::addAll)
                    .build();
            // final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
            // record.getSchema().getEntries().stream().filter(e -> !e.getName().equals("id")).forEach(item ->
            // addItem(recordBuilder, item, record));
            // final Record copy = recordBuilder.build();

            // get element name
            String jsonElementName;
            if (configuration.getMagentoDataSet().getSelectionType() == SelectionType.PRODUCTS) {
                jsonElementName = "product";
            } else {
                throw new RuntimeException("Selection type is not set");
            }

            final JsonObject copyWrapped = jsonBuilderFactory.createObjectBuilder().add(jsonElementName, copy).build();

            String magentoUrl = configuration.getMagentoDataSet().getMagentoUrl();
            JsonObject newJsonObject = magentoHttpClientService
                    .postRecords(configuration.getMagentoDataSet().getMagentoDataStore(), magentoUrl, copyWrapped);

            Record newRecord = magentoCmsService.jsonObjectToRecord(newJsonObject,
                    configuration.getMagentoDataSet().getSelectionType());
            success.emit(newRecord);
        } catch (HttpException httpError) {
            int status = httpError.getResponse().status();
            final JsonObject error = (JsonObject) httpError.getResponse().error(JsonObject.class);
            if (error != null && error.containsKey("message")) {
                reject.emit(new Reject(status, error.getString("message"), "", record));
            } else {
                reject.emit(new Reject(status, "unknown", "", record));
            }
        } catch (BadCredentialsException e) {
            log.error("Bad user credentials");
        } catch (BadRequestException e) {
            log.warn(e.getMessage());
            reject.emit(new Reject(400, e.getMessage(), "", record));
        }
    }
}