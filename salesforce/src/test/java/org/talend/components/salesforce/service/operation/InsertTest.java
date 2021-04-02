package org.talend.components.salesforce.service.operation;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.service.operation.converters.SObjectConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class InsertTest {

    private Map<String, Field> fields = new HashMap<>();

    @Test
    void execute() throws IOException {

        Field f1 = new Field();
        f1.setName("f1");
        f1.setType(FieldType.string);
        fields.put("f1", f1);

        Field f2 = new Field();
        f2.setName("f2");
        f2.setType(FieldType._int);
        fields.put("f2", f2);

        final SObjectConverter converter = new SObjectConverter( () -> this.fields, "moduleTest");
        final Insert insert = new Insert(facade, converter);

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Record r1 = factory.newRecordBuilder() //
                .withString("f1", "f1value") //
                .withInt("f2", 123) //
                .build();

        final List<Result> results = insert.execute(Arrays.asList(r1));
        Assertions.assertTrue(results.get(0).isOK());
        Assertions.assertNotNull(this.sObject);
        final Object fieldF1 = this.sObject.getField("f1");
        final Object fieldF2 = this.sObject.getField("f2");
        Assertions.assertEquals("f1value", fieldF1);
        Assertions.assertEquals(123, fieldF2);
    }

    SObject sObject = null;

    private final ConnectionFacade facade = new ConnectionFacade() {

        @Override
        public SaveResult[] create(SObject[] sObjects) throws ConnectionException {
            SaveResult[] r = new SaveResult[] {
                    new SaveResult()
            };
            r[0].setSuccess(true);
            InsertTest.this.sObject = sObjects[0];
            return r;
        }

        @Override
        public DeleteResult[] delete(String[] ids) throws ConnectionException {
            return new DeleteResult[0];
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