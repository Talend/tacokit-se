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
import org.talend.components.mongodb.service.ClientOptionsFactory;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.components.mongodb.source.MongoDBInputMapperConfiguration;

public class InputClientOptionsFactory extends ClientOptionsFactory {

    private final MongoDBInputMapperConfiguration properties;

    public InputClientOptionsFactory(final MongoDBInputMapperConfiguration properties, final I18nMessage i18nMessage) {
        super(properties.getDataset().getDatastore(), i18nMessage);
        this.properties = properties;
    }

    @Override
    protected void setSpecificOptions(final MongoClientOptions.Builder builder) {
        if (properties.isSetReadPreference() && properties.getReadPreference() != null) {
            builder.readPreference(convertReadPreference(properties.getReadPreference())).build();
        }
    }

    private ReadPreference convertReadPreference(MongoDBInputMapperConfiguration.ReadPreference readPreference) {
        switch (readPreference) {
        case NEAREST:
            return ReadPreference.nearest();
        case PRIMARY:
            return ReadPreference.primary();
        case SECONDARY:
            return ReadPreference.secondary();
        case PRIMARY_PREFERRED:
            return ReadPreference.primaryPreferred();
        case SECONDARY_PREFERRED:
            return ReadPreference.secondaryPreferred();
        default:
            throw new IllegalArgumentException(i18nMessage.unknownReadPreference(String.valueOf(readPreference)));
        }
    }

}
