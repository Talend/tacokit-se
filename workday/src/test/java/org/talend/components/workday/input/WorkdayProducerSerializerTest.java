package org.talend.components.workday.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.components.workday.service.WorkdayReaderService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.time.Instant;

class WorkdayProducerSerializerTest {

    @Test
    public void serialProducer() throws Exception {
        InputConfiguration cfg = new InputConfiguration();
        WorkdayReaderService service = new WorkdayReaderService();

        WorkdayDataSet ds = new WorkdayDataSet();
        cfg.setDataSet(ds);
        ds.setService("hello/v1/xx");
        WorkdayDataStore store = new WorkdayDataStore();
        ds.setDatastore(store);
        store.setTenantAlias("myalias");
        store.setEndpoint("http://myendpoint");
        store.setClientId("clientid");
        store.setClientSecret("secret");
        store.setAuthEndpoint("http://myauthendpoint");
        store.setToken(new Token("123", "auth", Instant.now()));

        WorkdayProducer producer = new WorkdayProducer(cfg, service);
        producer.init();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bytes);
        output.writeObject(producer);

        ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
        ObjectInputStream input = new ObjectInputStream(in);
        Object o = input.readObject();

        Assertions.assertNotNull(o);
        Assertions.assertEquals(WorkdayProducer.class, o.getClass());

        WorkdayProducer producer1 = (WorkdayProducer) o;

        Field cfgField = WorkdayProducer.class.getDeclaredField("inputConfig");
        cfgField.setAccessible(true);
        Object objCfg = cfgField.get(producer1);
        Assertions.assertNotNull(objCfg);

        InputConfiguration cfg2 = (InputConfiguration) objCfg;
        Assertions.assertEquals(cfg, cfg2);
    }


}