package org.talend.components.cosmosDB;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.talend.components.cosmosDB.input.CosmosDBInput;
import org.talend.components.cosmosDB.input.CosmosDBInputConfiguration;
import org.talend.sdk.component.api.record.Record;

public class CosmosInputTestIT extends CosmosDbTestBase {


    CosmosDBInputConfiguration config;

    @Before
    public void prepare() {
        super.prepare();
        config = new CosmosDBInputConfiguration();
        dataSet.setUseQuery(true);
        dataSet.setQuery("SELECT {\"city\":c.address.city} from c");
        config.setDataset(dataSet);

    }
    @Test
    public void outputTest() {


        CosmosDBInput input = new CosmosDBInput(config,service,recordBuilderFactory,i18n);
        input.init();
        Record next = input.next();
        System.out.println(next);
        input.release();
        Assert.assertNotNull(next);

    }
}
