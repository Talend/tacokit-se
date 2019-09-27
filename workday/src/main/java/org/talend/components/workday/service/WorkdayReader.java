package org.talend.components.workday.service;

import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;

/**
 * Service data getter from workday.
 */
public interface WorkdayReader extends HttpClient {

    /**
     *
     * @param token : authorize token.
     * @param path : final path for workday REST service (common/v1/workers ...).
     * @param offset : The offset in the collection.
     * @param limit : The number of instances to retrieve.
     * @return
     */
    @Request(path = "/{service}", method = "GET")
    Response<JsonObject> search(@Header("Authorization") String token,
                                @Path("service") String servicePath,
                                @Query("offset") int offset,
                                @Query("limit") int limit);
}
