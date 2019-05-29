package org.talend.components.onedrive.sources.get;

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
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_get")
@Processor(name = "Get")
@Documentation("Data deletion processor")
public class OneDriveGetSource implements Serializable {

    private final OneDriveGetConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    public OneDriveGetSource(@Option("configuration") final OneDriveGetConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        processOutputElement(configuration, record, success, reject);
    }

    private void processOutputElement(OneDriveGetConfiguration configuration, final JsonObject record,
            OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        String itemId = record.getString("id");
        try {
            JsonObject newRecord = oneDriveHttpClientService.getItemData(configuration, itemId);
            success.emit(newRecord);
        } catch (Exception e) {
            CommonHelper.processException(e, record, reject);
        }
    }
}