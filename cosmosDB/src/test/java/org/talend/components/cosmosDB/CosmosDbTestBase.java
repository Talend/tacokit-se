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
package org.talend.components.cosmosDB;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.talend.components.cosmosDB.dataset.QueryDataset;
import org.talend.components.cosmosDB.datastore.CosmosDBDataStore;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.components.cosmosDB.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ServiceInjectionRule;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Environment(DirectRunnerEnvironment.class)
@Slf4j
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

    public static String databaseID;

    public static String collectionID;

    protected static final String uuid = UUID.randomUUID().toString().replaceAll("-", "");

    protected static CosmosTestUtils cosmosTestUtils;

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
        primaryKey = System.getProperty("cosmos.primaryKey", "");
        serviceEndpoint = System.getProperty("cosmos.serviceEndpoint", "accountKey");
        databaseID = "database_" + uuid;
        collectionID = "collection_" + uuid;

        System.setProperty("talend.junit.http.capture", "true");
    }

    protected CosmosDBDataStore dataStore;

    protected QueryDataset dataSet;

    @BeforeClass
    public static void prepareDatabse() throws IOException, DocumentClientException {

        DocumentClient client = new DocumentClient(serviceEndpoint, primaryKey, new ConnectionPolicy(), ConsistencyLevel.Session);

        cosmosTestUtils = new CosmosTestUtils(client, databaseID, collectionID);
        cosmosTestUtils.createDatabaseIfNotExists();
        cosmosTestUtils.createDocumentCollectionIfNotExists();
        cosmosTestUtils.insertDocument(
                "{\"lastName\":\"Wakefield\",\"address\":{\"city\":\"NY\",\"county\":\"Manhattan\",\"state\":\"NY\"},\"children\":[{\"pets\":[{\"givenName\":\"Goofy\"},{\"givenName\":\"Shadow\"}],\"firstName\":\"Jesse\",\"gender\":null,\"familyName\":\"Merriam\",\"grade\":8},{\"pets\":null,\"firstName\":\"Lisa\",\"gender\":\"female\",\"familyName\":\"Miller\",\"grade\":1}],\"district\":\"NY23\",\"registered\":true,\"id\":\"Wakefield.7\",\"parents\":[{\"firstName\":\"Robin\",\"familyName\":\"Wakefield\"},{\"firstName\":\"Ben\",\"familyName\":\"Miller\"}]}");
        cosmosTestUtils.insertDocument(
                "{\"lastName\":\"Andersen\",\"address\":{\"city\":\"Seattle\",\"county\":\"King\",\"state\":\"WA\"},\"children\":null,\"district\":\"WA5\",\"registered\":true,\"id\":\"Andersen.1\",\"parents\":[{\"firstName\":\"Thomas\",\"familyName\":null},{\"firstName\":\"MaryKay\",\"familyName\":null}]}");

    }

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
        dataStore.setDatabaseID(databaseID);
        dataSet = new QueryDataset();
        dataSet.setDatastore(dataStore);
        dataSet.setCollectionID(collectionID);

    }

    @AfterClass
    public static void dropDatabase() throws DocumentClientException {
        cosmosTestUtils.dropDatabase();
    }

    protected List<Record> createData(int i) {
        List records = new ArrayList(i);
        for (; i > 0; i--) {
            Record record = recordBuilderFactory.newRecordBuilder() //
                    .withInt("id2", i) //
                    .withString("id", "" + i).withString("lastName", "firstfirst") //
                    .withDouble("double", 3.555) //
                    .withLong("long", 7928342L) //
                    .withInt("int", 3242342) //
                    .withRecord("record", createData2(1).get(0)) //
                    .withBytes("bytes", "YOasdfe2232".getBytes()).withDateTime("Date1", new Date(435352454530l)).build();

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
                .withInt("sdfds", 1) //
                .withString("id", "Andersen.1") //
                .withString("address", "444") //
                .withString("enrolled", "Datedsldsk") //
                .withString("zip", "89100") //
                .withString("lastName", "Andersen") //
                .build();
        records.add(record);
        Record record2 = recordBuilderFactory.newRecordBuilder() //
                .withInt("id", 2) //
                .withString("firstname", "firstfirst") //
                .withString("lastname", "lastlast") //
                .withString("address", "444") //
                .withString("enrolled", "Dated,sldsk") //
                .withString("zip", "89100") //
                .withString("state", "YO") //
                .build();
        records.add(record2);

        return records;
    }

    protected boolean recordEqual(Record record, Document document) {
        Schema schema = record.getSchema();
        List<Schema.Entry> entries = schema.getEntries();
        boolean result = true;
        Base64.Decoder decoder = Base64.getDecoder();
        for (Schema.Entry entry : entries) {
            switch (entry.getType()) {
            case BYTES:
                byte[] decode = decoder.decode(String.valueOf(document.get(entry.getName())).getBytes());
                boolean equals1 = new String(record.getBytes(entry.getName())).equals(new String(decode));
                result = result && equals1;
                break;
            case DATETIME:
                String format = record.getDateTime(entry.getName()).format(DateTimeFormatter.ISO_DATE_TIME);
                result = result && format.equals(document.get(entry.getName()));
                break;
            case LONG:
                Long aLong = record.getLong(entry.getName());
                result = result && aLong.equals(Long.valueOf(String.valueOf(document.get(entry.getName()))));
                break;
            case RECORD:
                ;
                result = result
                        && recordEqual(record.getRecord(entry.getName()), new Document(document.get(entry.getName()).toString()));
                break;

            default:
                Object o = record.get(getEntryClass(entry), entry.getName());
                if (o != null) {
                    result = result && o.equals(document.get(entry.getName()));
                } else {
                    result = result && (document.get(entry.getName()) == null);
                }
            }
        }
        return result;
    }

    public Class getEntryClass(Schema.Entry entry) {

        switch (entry.getType()) {
        case STRING:
            return String.class;
        case INT:
            return Integer.class;
        case RECORD:
            return Object.class;
        case LONG:
            return Long.class;
        case DATETIME:
            return ZonedDateTime.class;
        case ARRAY:
            return Collection.class;
        case FLOAT:
        case DOUBLE:
            return Double.class;
        case BOOLEAN:
            return Boolean.class;
        case BYTES:
            return byte[].class;
        default:
            return String.class;

        }

    }

}
