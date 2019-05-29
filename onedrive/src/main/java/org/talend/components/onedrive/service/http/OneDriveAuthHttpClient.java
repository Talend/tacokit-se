package org.talend.components.onedrive.service.http;

import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

import javax.json.JsonObject;

public interface OneDriveAuthHttpClient extends HttpClient {

    String HEADER_Content_Type = "Content-Type";

    @Request(method = "POST", path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonObject> getToken(@Path("requestPath") String requestPath, @Header(HEADER_Content_Type) String contentType,
            String body);

    default Response<JsonObject> getToken(String requestPath, String body) {
        return getToken(requestPath, "application/x-www-form-urlencoded", body);
    }

}
