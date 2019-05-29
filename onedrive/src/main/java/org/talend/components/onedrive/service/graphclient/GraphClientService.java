package org.talend.components.onedrive.service.graphclient;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReaderFactory;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class GraphClientService {

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private AuthorizationHelper authorizationHelper;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    private Map<OneDriveDataStore, GraphClient> graphClients = new ConcurrentHashMap<>();

    public GraphClient getGraphClient(OneDriveDataStore oneDriveDataStore) {
        return graphClients.computeIfAbsent(oneDriveDataStore, key -> new GraphClient(oneDriveDataStore, authorizationHelper));
    }

    public JsonObject driveItemToJson(DriveItem item) {
        String jsonInString = item.getRawObject().toString();
        JsonObject jsonObject = jsonReaderFactory.createReader(new StringReader(jsonInString)).readObject();
        jsonObject = removeBadKeys(jsonObject);
        return jsonObject;
    }

    private JsonObject removeBadKeys(JsonObject jsonObject) {
        final JsonObjectBuilder objectBuilder = jsonBuilderFactory.createObjectBuilder(jsonObject);
        jsonObject.keySet().forEach(key -> {
            if (key.startsWith("@"))
                objectBuilder.remove(key);
        });
        return objectBuilder.build();
    }
}
