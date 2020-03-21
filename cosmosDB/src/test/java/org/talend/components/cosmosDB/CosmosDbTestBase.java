/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.cosmosDB;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import org.talend.components.cosmosDB.dataset.CosmosDBDataset;
import org.talend.components.cosmosDB.dataset.QueryDataset;
import org.talend.components.cosmosDB.datastore.CosmosDBDataStore;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.components.cosmosDB.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ServiceInjectionRule;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;

@Environment(DirectRunnerEnvironment.class)
public class CosmosDbTestBase {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.cosmosDB");

    @Rule
    public final ServiceInjectionRule injections = new ServiceInjectionRule(COMPONENT_FACTORY, this);

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    @Service
    protected CosmosDBService service;

    @Service
    protected I18nMessage i18n;

    public static String accountName;

    public static String primaryKey;

    public static String serviceEndpoint;

    public static String database;

    public static String collectionID;

    public static String tenantId;

    public static String sas;

    public static String password;

    public static String BLOBAccountName;

    public static String BLOBAccountKey;

    public static String SQLDWHUrl;

    static {
        Properties prop = new Properties();
        java.io.InputStream input = null;
        try {
            input = new FileInputStream(System.getenv("ENV") + "/tacokit_properties.txt");
            prop.load(input);
            // System.setProperties(prop);
            for (String name : prop.stringPropertyNames()) {
                System.setProperty(name, prop.getProperty(name));
            }
        } catch (java.io.IOException ex) {
            System.err.println("Did not find azure properties, you can still pass them with -D");
        }
        accountName = System.getProperty("cosmos.accountName", "pyzhou");
        primaryKey = System.getProperty("cosmos.primaryKey", "");
        serviceEndpoint = System.getProperty("cosmos.serviceEndpoint", "accountKey");
        database = System.getProperty("cosmos.databaseID", "pyzhou");
        collectionID = System.getProperty("cosmos.collectionID", "secret");
        tenantId = System.getProperty("adlsgen2.tenantId", "talendId");
        sas = System.getProperty("adlsgen2.sas", "ZZZ_SAS");
        password = System.getProperty("SQLDWH.password");
        SQLDWHUrl = System.getProperty("SQLDWH.url");
        BLOBAccountName = System.getProperty("BLOB.accountName");
        BLOBAccountKey = System.getProperty("BLOB.accountKey");

        System.setProperty("talend.junit.http.capture", "true");
    }

    protected CosmosDBDataStore dataStore;

    protected QueryDataset dataSet;

    @Before
    public void prepare() {
        Properties properties = System.getProperties();
        properties.stringPropertyNames();
        for (String property : properties.stringPropertyNames()) {
            System.out.println(property + " : " + System.getProperty(property));
        }

        dataStore = new CosmosDBDataStore();
        dataStore.setServiceEndpoint(serviceEndpoint);
        dataStore.setPrimaryKey(primaryKey);
        dataStore.setDatabaseID(database);
        dataSet = new QueryDataset();
        dataSet.setDatastore(dataStore);
        dataSet.setCollectionID(collectionID);

    }

    protected List<Record> createData(int i) {
        List records = new ArrayList(i);
        for (; i > 0; i--) {
            Record record = recordBuilderFactory.newRecordBuilder() //
                    .withInt("id2", i) //
                    .withString("id", "" + i).withString("firstname", "firstfirst") //
                    .withDouble("double", 3.555) //
                    .withLong("long", 7928342L) //
                    .withInt("int", 3242342) //
                    // .withRecord("record", createData2(1).get(0)) //
                    .withBytes("bytes", "YO".getBytes()).withDateTime("Date1", new Date()).build();
            records.add(record);
        }
        return records;
    }

    protected List<Record> createData2(int i) {
        List records = new ArrayList(i);
        for (; i > 0; i--) {
            Record record = recordBuilderFactory.newRecordBuilder() //
                    .withInt("id", i) //
                    .withString("firstname", "firstfirst") //
                    .withString("quoter", "\"\"").withString("nullString", "").withString("null", null).build();
            records.add(record);
        }
        return records;
    }

    protected List<Record> createData3() {
        List records = new ArrayList();
        Record record = recordBuilderFactory.newRecordBuilder() //
                .withInt("id", 1) //
                .withString("firstname", "firstfirst") //
                .withString("lastname", "lastlast") //
                .withString("address", "addressaddr") //
                .withString("enrolled", "Datedsldsk") //
                .withString("zip", "89100") //
                .withString("state", "YO") //
                .build();
        records.add(record);
        Record record2 = recordBuilderFactory.newRecordBuilder() //
                .withInt("id", 2) //
                .withString("firstname", "firstfirst") //
                .withString("lastname", "lastlast") //
                .withString("address", "addressaddr") //
                .withString("enrolled", "Dated,sldsk") //
                .withString("zip", "89100") //
                .withString("state", "YO") //
                .build();
        records.add(record2);

        return records;
    }

}
