package org.talend.components.onedrive.sources.delete;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.CommonHelper;
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
import java.io.Serializable;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_delete")
@Processor(name = "Delete")
@Documentation("Data deletion processor")
public class OneDriveDeleteSource implements Serializable {

    private final OneDriveDeleteConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    public OneDriveDeleteSource(@Option("configuration") final OneDriveDeleteConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        String itemId = record.getString("id");
        try {
            oneDriveHttpClientService.deleteItem(configuration.getDataSet().getDataStore(), itemId);
            success.emit(record);
        } catch (Exception e) {
            CommonHelper.processException(e, record, reject);
        }
    }
}