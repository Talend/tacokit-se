package org.talend.components.salesforce.service.operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

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
        public SaveResult[] create(SObject[] sObjects) throws ConnectionException {
            return new SaveResult[0];
        }

        @Override
        public DeleteResult[] delete(String[] ids) throws ConnectionException {
            final DeleteResult[] dr = new DeleteResult[] {
                    new DeleteResult()
            };
            dr[0].setSuccess(true);
            return dr;
        }

        @Override
        public SaveResult[] update(SObject[] sObjects) throws ConnectionException {
            return new SaveResult[0];
        }

        @Override
        public UpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects)
                throws ConnectionException {
            return new UpsertResult[0];
        }
    };
}