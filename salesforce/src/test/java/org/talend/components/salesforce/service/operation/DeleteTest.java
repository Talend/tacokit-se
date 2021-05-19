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
package org.talend.components.salesforce.service.operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.soap.partner.IUpsertResult;
import com.sforce.soap.partner.sobject.SObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class DeleteTest {

    @Test
    void execute() throws IOException {
        final Delete delete = new Delete(this.facade);

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Record r1 = factory.newRecordBuilder().withString("Id", "1234").build();
        final List<Result> results = delete.execute(Arrays.asList(r1));

        Assertions.assertTrue(results.get(0).isOK());
    }

    ConnectionFacade facade = new ConnectionFacade() {

        @Override
        public ISaveResult[] create(SObject[] sObjects) {
            return new ISaveResult[0];
        }

        @Override
        public DeleteResult[] delete(String[] ids) {
            final DeleteResult[] dr = new DeleteResult[] { new DeleteResult() };
            dr[0].setSuccess(true);
            return dr;
        }

        @Override
        public ISaveResult[] update(SObject[] sObjects) {
            return new ISaveResult[0];
        }

        @Override
        public IUpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects) {
            return new IUpsertResult[0];
        }
    };
}