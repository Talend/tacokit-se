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
package org.talend.components.salesforce.output;

import java.io.IOException;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.IDeleteResult;
import com.sforce.soap.partner.IDescribeSObjectResult;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.IField;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.soap.partner.IUpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.components.salesforce.service.operation.ConnectionFacade;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@WithComponents("org.talend.components.salesforce")
class SalesforceOutputTest {

    @Service
    private LocalConfiguration configuration;

    @Service
    private Messages messages;

    @Service
    private RecordBuilderFactory factory;

    @Test
    void onNext() throws IOException {
        // init
        final OutputConfig config = new OutputConfig();
        config.setOutputAction(OutputConfig.OutputAction.INSERT);
        config.setCommitLevel(1);
        config.setBatchMode(true);

        final ModuleDataSet dataset = new ModuleDataSet();
        config.setModuleDataSet(dataset);
        dataset.setModuleName("moduleTest");

        final BasicDataStore dataStore = new BasicDataStore();
        dataset.setDataStore(dataStore);

        // test OK
        FakeISaveResult resultModel = new FakeISaveResult();
        resultModel.setSuccess(true);
        resultModel.setId("OK");
        final SalesforceServiceFake service = new SalesforceServiceFake(resultModel);

        final SalesforceOutput output = new SalesforceOutput(config, this.configuration, service, this.messages);

        final Schema schema = this.buildSchema();
        final Record r1 = this.buildRecord(schema, "value1", 1);
        final Record r2 = this.buildRecord(schema, "value2", 1);

        output.onNext(r1);
        output.onNext(r2);
        output.release();

        // KO
        resultModel.setSuccess(false);
        Error err = new Error();
        err.setMessage("test err msg");
        resultModel.setErrors(new IError[] { err });
        final SalesforceOutput outputErr = new SalesforceOutput(config, this.configuration, service, this.messages);
        outputErr.onNext(r1);
        outputErr.onNext(r2);
        outputErr.release();
    }

    private Record buildRecord(final Schema schema, final String e1Value, final int e2Value) {
        return this.factory.newRecordBuilder(schema) //
                .withString("e1", e1Value) //
                .withInt("e2", e2Value) //
                .build();
    }

    private Schema buildSchema() {
        final Schema.Entry e1 = this.factory.newEntryBuilder().withName("e1").withNullable(true).withType(Schema.Type.STRING)
                .build();
        final Schema.Entry e2 = this.factory.newEntryBuilder().withName("e2").withNullable(true).withType(Schema.Type.INT)
                .build();
        return factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(e1).withEntry(e2).build();
    }

    @RequiredArgsConstructor
    static class SalesforceServiceFake extends SalesforceService {

        private final ISaveResult model;

        @Override
        public ConnectionFacade buildConnection(final BasicDataStore datastore, final LocalConfiguration localConfiguration)
                throws ConnectionException {
            return new FakeCnxFacade(this.model);
        }
    }

    @RequiredArgsConstructor
    static class FakeCnxFacade implements ConnectionFacade {

        private final ISaveResult model;

        @Override
        public ISaveResult[] create(SObject[] sObjects) throws ConnectionException {
            if (sObjects == null || sObjects.length == 0) {
                return new ISaveResult[0];
            }
            final ISaveResult[] saveResult = new ISaveResult[sObjects.length];
            for (int i = 0; i < saveResult.length; i++) {
                saveResult[i] = model;
            }
            return saveResult;
        }

        @Override
        public IDeleteResult[] delete(String[] ids) throws ConnectionException {
            return new IDeleteResult[0];
        }

        @Override
        public ISaveResult[] update(SObject[] sObjects) throws ConnectionException {
            return new ISaveResult[0];
        }

        @Override
        public IUpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects) throws ConnectionException {
            return new IUpsertResult[0];
        }

        @Override
        public IDescribeSObjectResult describeSObject(String sObjectType) throws ConnectionException {
            final DescribeSObjectResult result = new DescribeSObjectResult();
            final IField[] fields = new IField[2];
            fields[0] = this.newField(FieldType.string, "e1");
            fields[1] = this.newField(FieldType._int, "e2");

            result.setFields(fields);
            return result;
        }

        private IField newField(final FieldType ft, final String name) {
            Field f = new Field();
            f.setType(ft);
            f.setName(name);
            return f;
        }
    }

    @Data
    static class FakeISaveResult implements ISaveResult {

        private boolean success;

        private String id;

        private IError[] errors;

        @Override
        public boolean getSuccess() {
            return success;
        }

        @Override
        public boolean isSuccess() {
            return success;
        }
    }
}