package org.talend.components.onedrive.sources.list;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.list.iterator.DriveItemWrapper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Slf4j
@Documentation("Input data processing class")
public class OneDriveListSource implements Serializable {

    private final OneDriveListConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    private Iterator<DriveItem> inputIterator;

    private GraphClientService graphClientService;

    public OneDriveListSource(@Option("configuration") final OneDriveListConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService, GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
    }

    @PostConstruct
    public void init() throws IOException {
        DriveItem item = oneDriveHttpClientService.getItemByPath(configuration.getDataSet().getDataStore(),
                configuration.getObjectPath());
        if (configuration.isRecursively()) {
            DriveItemWrapper itemWrapper = new DriveItemWrapper(configuration.getDataSet().getDataStore(),
                    oneDriveHttpClientService, item);
            inputIterator = itemWrapper;
        } else {
            inputIterator = Arrays.asList(item).stream().iterator();
        }
    }

    @Producer
    public JsonObject next() {
        if (inputIterator != null) {
            try {
                DriveItem item = inputIterator.next();
                if (item == null) {
                    return null;
                }
                log.debug("parent path: " + item.parentReference.path);
                JsonObject res = graphClientService.driveItemToJson(item);

                return res;
            } catch (NoSuchElementException e) {
                return null;
            }
        }
        return null;
    }

}