package org.talend.components.mongodb.service;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.talend.components.mongodb.Messages;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.datastore.MongoDBDataset;
import org.talend.components.mongodb.input.MongoDBInputConfig;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.util.logging.Logger;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Slf4j
@Service
public class MongoDBService {

    private MongoClient mongo;

    public MongoClient getConnection(final MongoDBDataStore datasetore) {
        // disable the log4j of mongodb
        Logger.getLogger("org.mongodb.driver").setLevel(java.util.logging.Level.OFF);

        MongoClientOptions clientOptions =
                // datasetore.isUse_SSL()? new
                // MongoClientOptions.Builder().socketFactory(javax.net.ssl.SSLSocketFactory.getDefault()).build():
                new MongoClientOptions.Builder().build();

        ServerAddress serverAddress = new ServerAddress(datasetore.getServer(), Integer.valueOf(datasetore.getPort()));

        if (datasetore.isAuthentication()) {
            MongoCredential mongoCredential = null;
            String username = datasetore.getUsername();
            String database = datasetore.getDatabase();
            char[] pass = datasetore.getPassword().toCharArray();
            switch (datasetore.getAuthentication_mechanism()) {
            case NEGOTIATE_MEC:
                mongoCredential = MongoCredential.createCredential(username, database, pass);
                break;
            case PLAIN_MEC:
                mongoCredential = MongoCredential.createPlainCredential(username, database, pass);
                break;
            case SCRAMSHA1_MEC:
                mongoCredential = MongoCredential.createScramSha1Credential(username, database, pass);
                break;
            // case KERBEROS_MEC:
            // // TODO impliment
            // System.setProperty("java.security.krb5.realm", "krbRealm");
            // System.setProperty("java.security.krb5.kdc", "krbKdc");
            // System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            // mongoCredential = MongoCredential.createGSSAPICredential("krbUserPrincipal");
            // break;
            }
            mongo = new MongoClient(serverAddress, mongoCredential, clientOptions);
        } else {
            mongo = new MongoClient(serverAddress, clientOptions);
        }

        // TODO useReplicaSet

        mongo.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
         log.info("Connecting to " + mongo.getServerAddressList() + ".");

        return mongo;
    }

    public MongoCollection<Document> getCollection(final MongoDBDataset dataset) {
        MongoDatabase db = mongo.getDatabase(dataset.getDataStore().getDatabase());
        log.debug("Retrieving records from the datasource.");
        return db.getCollection(dataset.getCollection());
    }

    public void close() {
        if (mongo != null) {
            mongo.close();
        }
    }

    @HealthCheck("basic.healthcheck")
    public HealthCheckStatus testConnection(final MongoDBDataStore datasetore, final Messages i18n) {
        try {
            mongo = this.getConnection(datasetore);
            mongo.isLocked();
        } catch (Exception e) {
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(e.getMessage()));
        }

        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}
