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

package org.talend.components.mongodb.service;

import com.mongodb.MongoClientOptions;
import org.talend.components.mongodb.datastore.MongoDBDatastore;

public abstract class ClientOptionsFactory {

    private final MongoDBDatastore datastore;

    protected final I18nMessage i18nMessage;

    public ClientOptionsFactory(final MongoDBDatastore properties, final I18nMessage i18nMessage) {
        this.datastore = properties;
        this.i18nMessage = i18nMessage;
    }

    public MongoClientOptions createOptions() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        if (datastore.isUseSSL()) {
            builder.sslEnabled(true);
        }
        setSpecificOptions(builder);
        return builder.build();
    }

    protected abstract void setSpecificOptions(final MongoClientOptions.Builder builder);

}
