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
import org.talend.components.mongodb.service.ClientOptionsFactory;
import org.talend.components.mongodb.service.I18nMessage;

public class DefaultClientOptionsFactory extends ClientOptionsFactory {

    public DefaultClientOptionsFactory(final MongoDBDatastore datastore, final I18nMessage i18nMessage) {
        super(datastore, i18nMessage);
    }

    @Override
    public void setSpecificOptions(final MongoClientOptions.Builder builder) {
        // noop
    }
}
