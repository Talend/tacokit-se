package org.talend.components.adlsgen2.dataset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConfiguration;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;

class AdlsGen2DataSetTest {

    @Test
    void testSerial() throws IOException, ClassNotFoundException {
        final AdlsGen2Connection connection = new AdlsGen2Connection();
        connection.setAuthMethod(AuthMethod.ActiveDirectory);
        connection.setSas("sas");
        connection.setClientId("clientId");
        connection.setClientSecret("clientSecret");
        connection.setTenantId("tenant");
        connection.setTimeout(200);

        final AdlsGen2DataSet dataset = new AdlsGen2DataSet();
        dataset.setConnection(connection);
        dataset.setBlobPath("/blob/path");
        dataset.setCsvConfiguration(new CsvConfiguration());
        dataset.getCsvConfiguration().setEscapeCharacter("\\");

        dataset.setParquetConfiguration(new ParquetConfiguration());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos =  new ObjectOutputStream(out);
        oos.writeObject(dataset);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois =  new ObjectInputStream(input);
        final AdlsGen2DataSet dsCopy = (AdlsGen2DataSet) ois.readObject();
        Assertions.assertEquals(dataset, dsCopy);

    }
}