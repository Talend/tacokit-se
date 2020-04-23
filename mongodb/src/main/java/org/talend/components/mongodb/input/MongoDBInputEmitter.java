package org.talend.components.mongodb.input;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.bson.Document;
import org.talend.components.mongodb.Messages;
import org.talend.components.mongodb.service.MongoDBRecord;
import org.talend.components.mongodb.service.MongoDBService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.List;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "MongoDBInput")
@Emitter(name = "Input")
@Documentation("MongoDB query input ")
public class MongoDBInputEmitter implements Serializable {

    @Service
    private Messages i18n;

    private final MongoDBService service;

    private final MongoDBInputConfig inputConfig;

    private MongoCollection<Document> collection;

    private MongoClient mongo = null;

    private List<String> fields;

    MongoCursor<Document> cursor;

    // final private Messages i18n;

    public MongoDBInputEmitter(@Option("configuration") final MongoDBInputConfig inputConfig, final MongoDBService service) {
        this.service = service;
        this.inputConfig = inputConfig;
        this.fields = inputConfig.getDataset().getSchema();
    }

    @PostConstruct
    public void init() {
        mongo = service.getConnection(inputConfig.getDataset().getDataStore());
        collection = service.getCollection(inputConfig.getDataset());

        // ListIndexesIterable<DBObject> indexListIterable = collection.listIndexes(DBObject.class);
        boolean needIndexWarning = true;
        String indexList = "";
        for (DBObject index : collection.listIndexes(DBObject.class)) {
            for (String key : ((com.mongodb.DBObject) index.get("key")).keySet()) {
                // The regexp is:
                // - contain the db DBcolumnName between two backslashed quotes
                // - is followed at some point by a colon
                // - there is no comma between the the DBcolumnName and the colon
                if ((inputConfig.getQuery()).matches(".*" + key + "[^,]*:.*")) {
                    // We have an index, do not print error message
                    needIndexWarning = false;
                } else {
                    // This index is not in the query, add it into the indexList
                    indexList += ", " + key;
                }
            }

        }
        if ((!"".equals(indexList)) && (needIndexWarning)) {

            log.warn("tMongoDBInput_1 - The query does not contain any reference an index.  [" + indexList.substring(1) + " ]");

        }
        Document myQuery = Document.parse(inputConfig.getQuery());
        FindIterable<Document> fi = collection.find(myQuery).noCursorTimeout(false);
        // Map<String, String> pathMap = new HashMap<String, String>();
        // fi = fi.limit(inputConfig.getLimite());
        cursor = fi.iterator();

    }

    @Producer
    public IndexedRecord next() {

        if (cursor.hasNext()) {
            Document document = cursor.next();
            return new MongoDBRecord(document, fields);
        }

        return null;
    }

    private Object getValue(String parentNode, String currentName, Document dbObject) {
        // Used for Mapp only, not implement yet
        // Get the node value in embedded document,
        // If have no embedded document get root document node.
        Object value = null;
        if (dbObject == null) {
            return null;
        }
        if (parentNode == null || "".equals(parentNode)) {
            if ("*".equals(currentName)) {
                value = dbObject;
            } else if (dbObject.get(currentName) != null) {
                value = dbObject.get(currentName);
            }
        } else {
            String objNames[] = objNames = parentNode.split("\\.");
            Document currentObj = dbObject;
            for (int i = 0; i < objNames.length; i++) {
                currentObj = (Document) currentObj.get(objNames[i]);
                if (currentObj == null) {
                    break;
                }
            }
            if ("*".equals(currentName)) {
                value = currentObj;
            } else if (currentObj != null) {
                value = currentObj.get(currentName);
            }
        }
        return value;
    }

    @PreDestroy
    public void close() {
        service.close();
    }

}
