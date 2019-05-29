package org.talend.components.onedrive.helpers;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.sources.Reject;
import org.talend.sdk.component.api.processor.OutputEmitter;

import javax.json.JsonObject;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CommonHelper {

    public static void processException(Exception e, JsonObject record, OutputEmitter<Reject> reject) {
        log.warn(e.getMessage());
        Reject rejectObj = new Reject();
        rejectObj.setErrorMessage(e.getMessage());
        rejectObj.setRecord(record);
        reject.emit(rejectObj);
    }

}
