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
package org.talend.components.adlsgen2.dataset;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import io.delta.standalone.DeltaLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.delta.DeltaConfiguration;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConfiguration;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.manager.chain.Job;

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
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(dataset);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final AdlsGen2DataSet dsCopy = (AdlsGen2DataSet) ois.readObject();
        Assertions.assertEquals(dataset, dsCopy);

    }

}