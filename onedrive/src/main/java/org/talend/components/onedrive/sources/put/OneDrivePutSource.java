package org.talend.components.onedrive.sources.put;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.CommonHelper;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.Reject;
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Base64;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_put")
@Processor(name = "Put")
@Documentation("Data put processor")
public class OneDrivePutSource implements Serializable {

    private final OneDrivePutConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private GraphClientService graphClientService;

    public OneDrivePutSource(@Option("configuration") final OneDrivePutConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService, GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws IOException {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        log.debug("processOutputElement_local: ");
        try {
            DriveItem newItem;
            if (configuration.getDataSource() == OneDrivePutConfiguration.DataSource.File) {
                newItem = putLocalFile(record);
            } else {
                newItem = putBytes(record);
            }
            JsonObject newRecord = graphClientService.driveItemToJson(newItem);
            success.emit(newRecord);
        } catch (Exception e) {
            CommonHelper.processException(e, record, reject);
        }
    }

    private DriveItem putLocalFile(JsonObject record) throws IOException {
        String itemPath = record.getString("itemPath");
        String localPath = record.getString("localPath", null);

        if (localPath == null || localPath.isEmpty() || !new File(localPath).isFile()) {
            return oneDriveHttpClientService.putItemData(configuration.getDataSet().getDataStore(), itemPath, null, 0);
        } else {
            File f = new File(localPath);
            int fileLength = (int) f.length();
            try (InputStream inputStream = new FileInputStream(f)) {
                return oneDriveHttpClientService.putItemData(configuration.getDataSet().getDataStore(), itemPath, inputStream,
                        fileLength);
            }
        }
    }

    private DriveItem putBytes(JsonObject record) throws IOException {
        String itemPath = record.getString("itemPath");
        String payloadBase64 = record.getString("payload", null);

        if (payloadBase64 == null) {
            return oneDriveHttpClientService.putItemData(configuration.getDataSet().getDataStore(), itemPath, null, 0);
        } else {
            byte[] payload = Base64.getDecoder().decode(payloadBase64);
            int fileLength = payload.length;
            try (InputStream inputStream = new ByteArrayInputStream(payload)) {
                return oneDriveHttpClientService.putItemData(configuration.getDataSet().getDataStore(), itemPath, inputStream,
                        fileLength);
            }
        }
    }
}