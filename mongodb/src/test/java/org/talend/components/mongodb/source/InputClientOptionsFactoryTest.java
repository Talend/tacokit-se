/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

package org.talend.components.mongodb.source;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class InputClientOptionsFactoryTest {

    @Test
    public void testReadPreferences() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setDatastore(datastore);
        MongoDBInputMapperConfiguration configuration = new MongoDBInputMapperConfiguration();
        configuration.setDataset(dataset);
        configuration.setSetReadPreference(true);
        configuration.setReadPreference(MongoDBInputMapperConfiguration.ReadPreference.NEAREST);
        InputClientOptionsFactory clientOptionsFactory = new InputClientOptionsFactory(configuration, null);
        MongoClientOptions clientOptions = clientOptionsFactory.createOptions();

        assertThat(clientOptions.getReadPreference(), sameInstance(ReadPreference.nearest()));

        configuration.setReadPreference(MongoDBInputMapperConfiguration.ReadPreference.PRIMARY);
        clientOptionsFactory = new InputClientOptionsFactory(configuration, null);
        clientOptions = clientOptionsFactory.createOptions();

        assertThat(clientOptions.getReadPreference(), sameInstance(ReadPreference.primary()));

        configuration.setReadPreference(MongoDBInputMapperConfiguration.ReadPreference.PRIMARY_PREFERRED);
        clientOptionsFactory = new InputClientOptionsFactory(configuration, null);
        clientOptions = clientOptionsFactory.createOptions();

        assertThat(clientOptions.getReadPreference(), sameInstance(ReadPreference.primaryPreferred()));

        configuration.setReadPreference(MongoDBInputMapperConfiguration.ReadPreference.SECONDARY);
        clientOptionsFactory = new InputClientOptionsFactory(configuration, null);
        clientOptions = clientOptionsFactory.createOptions();

        assertThat(clientOptions.getReadPreference(), sameInstance(ReadPreference.secondary()));

        configuration.setReadPreference(MongoDBInputMapperConfiguration.ReadPreference.SECONDARY_PREFERRED);
        clientOptionsFactory = new InputClientOptionsFactory(configuration, null);
        clientOptions = clientOptionsFactory.createOptions();

        assertThat(clientOptions.getReadPreference(), sameInstance(ReadPreference.secondaryPreferred()));
    }

}
