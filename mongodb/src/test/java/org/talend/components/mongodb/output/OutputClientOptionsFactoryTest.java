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

package org.talend.components.mongodb.output;

import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;

@WithComponents("org.talend.components.mongodb")
public class OutputClientOptionsFactoryTest {

    @Service
    private I18nMessage i18nMessage;

    @Test
    public void testSSL() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        configuration.getDataset().getDatastore().setUseSSL(true);

        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);
        MongoClientOptions clientOptions = factory.createOptions();

        assertTrue(clientOptions.isSslEnabled());
    }

    @Test
    public void testWriteConcernAcknowledged() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        configuration.setSetWriteConcern(true);
        configuration.setWriteConcern(MongoDBOutputConfiguration.WriteConcern.ACKNOWLEDGED);

        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);
        MongoClientOptions clientOptions = factory.createOptions();

        assertSame(WriteConcern.ACKNOWLEDGED, clientOptions.getWriteConcern(), "Write concern should be ACKNOWLEDGED");
    }

    @Test
    public void testWriteConcernJournaled() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        configuration.setSetWriteConcern(true);
        configuration.setWriteConcern(MongoDBOutputConfiguration.WriteConcern.JOURNALED);

        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);
        MongoClientOptions clientOptions = factory.createOptions();

        assertSame(WriteConcern.JOURNALED, clientOptions.getWriteConcern(), "Write concern should be JOURNALED");
    }

    @Test
    public void testWriteConcernUnacknowledged() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        configuration.setSetWriteConcern(true);
        configuration.setWriteConcern(MongoDBOutputConfiguration.WriteConcern.UNACKNOWLEDGED);

        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);
        MongoClientOptions clientOptions = factory.createOptions();

        assertSame(WriteConcern.UNACKNOWLEDGED, clientOptions.getWriteConcern(), "Write concern should be UNACKNOWLEDGED");
    }

    @Test
    public void testWriteConcernReplicaAcknowledged() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        configuration.setSetWriteConcern(true);
        configuration.setWriteConcern(MongoDBOutputConfiguration.WriteConcern.REPLICA_ACKNOWLEDGED);

        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);
        MongoClientOptions clientOptions = factory.createOptions();

        assertSame(WriteConcern.W2, clientOptions.getWriteConcern(), "Write concern should be REPLICA_ACKNOWLEDGED");
    }

    @Test
    public void testConvertWriteConcern() {
        MongoDBOutputConfiguration configuration = createConfiguration();
        OutputClientOptionsFactory factory = new OutputClientOptionsFactory(configuration, i18nMessage);

        assertSame(WriteConcern.ACKNOWLEDGED, factory.convertWriteConcern(MongoDBOutputConfiguration.WriteConcern.ACKNOWLEDGED));
        assertSame(WriteConcern.UNACKNOWLEDGED,
                factory.convertWriteConcern(MongoDBOutputConfiguration.WriteConcern.UNACKNOWLEDGED));
        assertSame(WriteConcern.JOURNALED, factory.convertWriteConcern(MongoDBOutputConfiguration.WriteConcern.JOURNALED));
        assertSame(WriteConcern.W2, factory.convertWriteConcern(MongoDBOutputConfiguration.WriteConcern.REPLICA_ACKNOWLEDGED));
    }

    private MongoDBOutputConfiguration createConfiguration() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setDatastore(datastore);
        MongoDBOutputConfiguration configuration = new MongoDBOutputConfiguration();
        configuration.setDataset(dataset);
        return configuration;
    }

}
