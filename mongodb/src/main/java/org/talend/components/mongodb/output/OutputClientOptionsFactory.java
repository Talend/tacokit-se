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
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;
import org.talend.components.mongodb.service.ClientOptionsFactory;
import org.talend.components.mongodb.service.I18nMessage;

public class OutputClientOptionsFactory extends ClientOptionsFactory {

    private final MongoDBOutputConfiguration properties;

    public OutputClientOptionsFactory(final MongoDBOutputConfiguration properties, final I18nMessage i18nMessage) {
        super(properties.getDataset().getDatastore(), i18nMessage);
        this.properties = properties;
    }

    @Override
    protected void setSpecificOptions(final MongoClientOptions.Builder builder) {
        if (properties.isSetWriteConcern() && properties.getWriteConcern() != null) {
            builder.writeConcern(convertWriteConcern(properties.getWriteConcern())).build();
        }
    }

    protected WriteConcern convertWriteConcern(MongoDBOutputConfiguration.WriteConcern writeConcern) {
        switch (writeConcern) {
        case ACKNOWLEDGED:
            return WriteConcern.ACKNOWLEDGED;
        case UNACKNOWLEDGED:
            return WriteConcern.UNACKNOWLEDGED;
        case JOURNALED:
            return WriteConcern.JOURNALED;
        case REPLICA_ACKNOWLEDGED:
            return WriteConcern.W2;
        default:
            throw new IllegalArgumentException(i18nMessage.unknownWriteConcern(String.valueOf(writeConcern)));
        }
    }

}
