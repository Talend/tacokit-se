/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.dynamicscrm.source;

import java.net.URI;
import java.util.Iterator;

import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.ms.crm.odata.QueryOptionConfig;

public class DynamicsCrmQueryResultsIterator implements Iterator<ClientEntity> {

    private final DynamicsCRMClient client;

    private final QueryOptionConfig queryOptionConfig;

    private URI nextPageUri;

    private Iterator<ClientEntity> entityIterator;

    public DynamicsCrmQueryResultsIterator(DynamicsCRMClient client, QueryOptionConfig queryOptionConfig,
            ClientEntitySet clientEntitySet) {
        this.entityIterator = clientEntitySet.getEntities().iterator();
        this.nextPageUri = clientEntitySet.getNext();
        this.client = client;
        this.queryOptionConfig = queryOptionConfig;
    }

    public DynamicsCrmQueryResultsIterator(DynamicsCRMClient client, QueryOptionConfig queryOptionConfig) {
        this.client = client;
        this.queryOptionConfig = queryOptionConfig;
        requestNext();
    }

    @Override
    public boolean hasNext() {
        return (entityIterator != null && entityIterator.hasNext()) || nextPageUri != null;
    }

    @Override
    public ClientEntity next() {
        if (entityIterator == null || (!entityIterator.hasNext() && nextPageUri != null)) {
            requestNext();
        }
        if (entityIterator.hasNext()) {
            return entityIterator.next();
        }
        return null;
    }

    private void requestNext() {
        ODataEntitySetRequest<ClientEntitySet> request = client.createEntityRetrieveRequest(queryOptionConfig);
        if (nextPageUri != null) {
            request.setURI(nextPageUri);
        }
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();
        ClientEntitySet entitySet = response.getBody();
        entityIterator = entitySet.getEntities().iterator();
        nextPageUri = entitySet.getNext();
    }
}
