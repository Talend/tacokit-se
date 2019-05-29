package org.talend.components.onedrive.sources.create;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.CommonHelper;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.Reject;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;
import java.io.Serializable;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_create")
@Processor(name = "Create")
@Documentation("Data create processor")
public class OneDriveCreateSource implements Serializable {

    private final OneDriveCreateConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private GraphClientService graphClientService;

    public OneDriveCreateSource(@Option("configuration") final OneDriveCreateConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService, GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        try {
            DriveItem newItem;
            if (configuration.isCreateDirectoriesByList()) {
                newItem = oneDriveHttpClientService.createItem(configuration.getDataSet().getDataStore(), null,
                        OneDriveObjectType.DIRECTORY, record.getString("objectPath"));
            } else {
                newItem = oneDriveHttpClientService.createItem(configuration.getDataSet().getDataStore(),
                        record.getString("parentId"), configuration.getObjectType(), configuration.getObjectPath());
            }
            JsonObject newRecord = graphClientService.driveItemToJson(newItem);
            success.emit(newRecord);
        } catch (Exception e) {
            CommonHelper.processException(e, record, reject);
        }
    }
}