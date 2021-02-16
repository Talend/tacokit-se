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
package org.talend.components.adlsgen2.runtime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.talend.components.common.Constants;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;

import com.google.common.base.Splitter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdlsDatastoreRuntimeInfo {

    @Getter
    private AdlsGen2Connection connection;

    private Map<String, String> adTokenMap;

    private Map<String, String> sasMap;

    private final AdlsActiveDirectoryService tokenProviderService;

    public AdlsDatastoreRuntimeInfo(AdlsGen2Connection connection, AdlsActiveDirectoryService tokenProviderService) {
        this.connection = connection;
        this.tokenProviderService = tokenProviderService;

        prepareRuntimeInfo(connection);
    }

    protected void prepareRuntimeInfo(AdlsGen2Connection connection) {
        this.connection = connection;
        fillSecretsMaps();
    }

    private void fillSecretsMaps() {
        adTokenMap = new HashMap<>();

        switch (connection.getAuthMethod()) {
        case SharedKey:
            break;
        case ActiveDirectory:
            String activeDirToken = Optional.ofNullable(tokenProviderService.getActiveDirAuthToken(connection))
                    .orElseThrow(() -> new IllegalStateException("Active directory authentication token can't be null"));

            adTokenMap.put(Constants.HeaderConstants.AUTHORIZATION, "Bearer " + activeDirToken);
            break;
        case SAS:
            sasMap = Splitter.on("&").withKeyValueSeparator("=").split(connection.getSas().substring(1));
            break;
        default:
            throw new IllegalArgumentException("Incorrect auth method was selected");
        }
    }

    public Map<String, String> getAdTokenMap() {
        return adTokenMap == null ? Collections.emptyMap() : adTokenMap;
    }

    public Map<String, String> getSASMap() {
        return sasMap == null ? Collections.emptyMap() : sasMap;
    }
}
