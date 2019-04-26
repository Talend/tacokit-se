package org.talend.components.rest.service;

import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;

import java.util.Collections;

public class RequestConfigBuilder {

    private RequestConfigBuilder() {
    }

    public static RequestConfig getEmptyRequestConfig() {
        RequestConfig config = new RequestConfig();

        Datastore datastore = new Datastore();

        Authentication authent = new Authentication();
        authent.setType(Authorization.AuthorizationType.NoAuth);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.RAW);
        body.setRawValue("");

        Dataset dataset = new Dataset();
        dataset.setDatastore(datastore);
        dataset.setAuthentication(authent);
        dataset.setBody(body);
        dataset.setHasQueryParams(false);
        dataset.setQueryParams(Collections.emptyList());
        dataset.setHasHeaders(false);
        dataset.setHeaders(Collections.emptyList());
        dataset.setHasPathParams(false);
        dataset.setPathParams(Collections.emptyList());

        config.setDataset(dataset);

        return config;
    }

}
