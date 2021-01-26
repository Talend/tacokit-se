package org.talend.components.marketo.dataset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeMode;
import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeRelative;
import org.talend.components.marketo.dataset.MarketoDataSet.LeadAction;
import org.talend.components.marketo.datastore.MarketoDataStore;

class MarketoInputConfigurationTest {


    @Test
    void serial() throws IOException, ClassNotFoundException {
        final MarketoDataStore connection = new MarketoDataStore();

        connection.setClientId("clientId");
        connection.setClientSecret("clientSecret");
        connection.setEndpoint("http://endpoint.com/");

        final MarketoDataSet dataset = new MarketoDataSet();
        dataset.setDataStore(connection);
        dataset.setDateTimeMode(DateTimeMode.relative);
        dataset.setListId("1,2,3");
        dataset.setFields(Arrays.asList("field1", "field2"));
        dataset.setLeadAction(LeadAction.getLeadActivity);
        dataset.setActivityTypeIds(Arrays.asList("Activity1", "Activity2"));
        dataset.setSinceDateTimeAbsolute("absTime");
        dataset.setSinceDateTimeRelative(DateTimeRelative.PERIOD_AGO_1Y);

        final MarketoInputConfiguration configuration = new MarketoInputConfiguration();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(configuration);

        final ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final MarketoInputConfiguration cfgCopy = (MarketoInputConfiguration) ois.readObject();
        Assertions.assertEquals(configuration, cfgCopy);

        Assertions.assertEquals(configuration.toString(), cfgCopy.toString());
    }
}