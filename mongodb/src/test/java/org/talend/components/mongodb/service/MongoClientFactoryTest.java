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

import com.mongodb.MongoCredential;
import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.datastore.KerberosCredentials;
import org.talend.components.mongodb.datastore.MongoDBDatastore;

import static com.mongodb.AuthenticationMechanism.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoClientFactoryTest {

    @Test
    public void testCreateDefaultClientFactory() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        datastore.setAuthentication(false);

        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, null, null);

        assertThat(factory, instanceOf(MongoClientFactory.DefaultMongoClientFactory.class));
    }

    @Test
    public void testCreateNegotiateClientFactory() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        datastore.setAuthentication(true);
        datastore.setAuthenticationMechanism(MongoDBDatastore.AuthenticationMechanism.NEGOTIATE_MEC);
        datastore.setUsername("User");
        datastore.setPassword("Password");
        datastore.setDatabase("Database");

        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, null, null);

        assertThat(factory, instanceOf(MongoClientFactory.NegotiateAuthMongoClientBuilder.class));

        MongoCredential credentials = ((MongoClientFactory.NegotiateAuthMongoClientBuilder) factory).getMongoCredentials();

        assertEquals(null, credentials.getAuthenticationMechanism());
    }

    @Test
    public void testCreatePlainClientFactory() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        datastore.setAuthentication(true);
        datastore.setAuthenticationMechanism(MongoDBDatastore.AuthenticationMechanism.PLAIN_MEC);
        datastore.setUsername("User");
        datastore.setPassword("Password");
        datastore.setDatabase("Database");

        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, null, null);

        assertThat(factory, instanceOf(MongoClientFactory.PlainAuthMongoClientFactory.class));

        MongoCredential credentials = ((MongoClientFactory.PlainAuthMongoClientFactory) factory).getMongoCredentials();

        assertEquals(PLAIN, credentials.getAuthenticationMechanism());

    }

    @Test
    public void testCreateScramSha1ClientFactory() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        datastore.setAuthentication(true);
        datastore.setAuthenticationMechanism(MongoDBDatastore.AuthenticationMechanism.SCRAMSHA1_MEC);
        datastore.setUsername("User");
        datastore.setPassword("Password");
        datastore.setDatabase("Database");

        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, null, null);

        assertThat(factory, instanceOf(MongoClientFactory.ScramSha1AuthMongoClientBuilder.class));

        MongoCredential credentials = ((MongoClientFactory.ScramSha1AuthMongoClientBuilder) factory).getMongoCredentials();

        assertEquals(SCRAM_SHA_1, credentials.getAuthenticationMechanism());
    }

    @Test
    public void testCreateKerberosClientFactory() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        datastore.setAuthentication(true);
        datastore.setAuthenticationMechanism(MongoDBDatastore.AuthenticationMechanism.KERBEROS_MEC);
        KerberosCredentials kerbCreds = new KerberosCredentials();
        kerbCreds.setKdcServer("Server");
        kerbCreds.setRealm("Realm");
        kerbCreds.setUserPrincipal("UserPrincipal");
        datastore.setKerberosCreds(kerbCreds);
        datastore.setDatabase("Database");

        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, null, null);

        assertThat(factory, instanceOf(MongoClientFactory.KerberosAuthMongoClientFactory.class));

        MongoCredential credentials = ((MongoClientFactory.KerberosAuthMongoClientFactory) factory).getMongoCredentials();

        assertEquals(GSSAPI, credentials.getAuthenticationMechanism());
    }
}
