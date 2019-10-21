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
package org.talend.components.extension.components;

import org.talend.components.extension.DatastoreRef;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

@Version
@Icon(value = Icon.IconType.DEFAULT)
@Emitter(name = "Valid")
@Documentation("Valid input component")
public class ValidComponent implements Serializable {

    private final MyConfig myConfig;

    public ValidComponent(MyConfig myConfig) {
        this.myConfig = myConfig;
    }

    @Producer
    public Record next() {
        return null;
    }

    public static class MyConfig {

        @Option
        private MyDataset dataset;

        @Option("datastore1")
        @DatastoreRef(configurationId = "datastore1Conf", filters = { @DatastoreRef.Filter(key = "type", value = "Oauth1"),
                @DatastoreRef.Filter(key = "type", value = "Oauth2"), })
        private MyDatastore1 datastore1;

        @Option("datastore2")
        @DatastoreRef(configurationId = "datastore2Conf")
        private MyDatastore2 datastore2;
    }

    @DataSet
    public static class MyDataset implements Serializable {

        @Option
        private MyDatastore1 datastore;

    }

    @DataStore
    public static class MyDatastore1 implements Serializable {

        @Option
        private String url;

        @Option
        private String user;

        @Option
        private String password;

        @Option
        private ConnectionType type = ConnectionType.Simple;

    }

    @DataStore("MyDatastore2")
    public static class MyDatastore2 implements Serializable {
    }

    enum ConnectionType {
        Simple,
        Oauth1,
        Oauth2,
        Saml;
    }
}
