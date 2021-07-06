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
package org.talend.components.salesforce.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.IDeleteResult;
import com.sforce.soap.partner.IField;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.soap.partner.IUpsertResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.components.salesforce.configuration.OutputConfig.OutputAction;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.service.operation.ConnectionFacade;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

@WithComponents("org.talend.components.salesforce")
class SalesforceOutputServiceTest {

    @Service
    private Messages messages;

    final List<SObject> objects = new ArrayList<>();

    final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void write() throws IOException {
        final OutputConfig cfg = new OutputConfig();
        final ConnectionFacade cnx = this.buildCnx();
        cfg.setCommitLevel(3);
        cfg.setBatchMode(true);
        cfg.setOutputAction(OutputAction.INSERT);
        final ModuleDataSet ds = new ModuleDataSet();
        cfg.setModuleDataSet(ds);
        ds.setModuleName("m1");

        final SalesforceOutputService output = new SalesforceOutputService(cfg, cnx, messages);
        Map<String, IField> fieldMap = new HashMap<>();

        final Field f1 = new Field();
        f1.setName("f1");
        f1.setType(FieldType.string);
        fieldMap.put("f1", f1);
        final Field f2 = new Field();
        f2.setName("f2");
        f2.setType(FieldType._int);
        fieldMap.put("f2", f2);

        output.setFieldMap(fieldMap);
        output.write(this.buildRecord(1));
        Assertions.assertTrue(this.objects.isEmpty());

        output.write(this.buildRecord(2));
        Assertions.assertTrue(this.objects.isEmpty());

        output.write(this.buildRecord(3));
        Assertions.assertEquals(3, this.objects.size());
        final SObject sObject1 = this.objects.get(0);
        Assertions.assertNotNull(sObject1);
        Assertions.assertEquals("m1", sObject1.getType());
        Assertions.assertEquals("value1", sObject1.getField("f1"));
        Assertions.assertEquals(1, sObject1.getField("f2"));
    }

    private Record buildRecord(int index) {
        return this.factory.newRecordBuilder().withString("f1", "value" + index).withInt("f2", index).build();
    }

    private ConnectionFacade buildCnx() {
        ConnectionFacade cnx = new ConnectionFacade() {

            @Override
            public ISaveResult[] create(SObject[] sObjects) throws ConnectionException {
                if (sObjects == null || sObjects.length == 0) {
                    return new ISaveResult[0];
                }

                ISaveResult[] res = new ISaveResult[sObjects.length];
                for (int i = 0; i < sObjects.length; i++) {
                    SalesforceOutputServiceTest.this.objects.add(sObjects[i]);
                    res[i] = new SaveResult();
                    res[i].setId("id" + i);
                    res[i].setSuccess(true);
                }
                return res;
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
        };
        return cnx;
    }
}