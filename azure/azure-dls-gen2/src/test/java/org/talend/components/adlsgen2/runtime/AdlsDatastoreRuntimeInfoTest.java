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

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.FakeActiveDirectoryService;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.common.Constants;

class AdlsDatastoreRuntimeInfoTest {

    @Test
    void runtimeInfoSAS() {
        final AdlsDatastoreRuntimeInfo info = this.buildRuntimeInfo(AuthMethod.SAS);

        Assertions.assertTrue(info.getAdTokenMap().isEmpty());
        final Map<String, String> sasMap = info.getSASMap();
        Assertions.assertEquals("2019-12-12", sasMap.get("sv"));
        Assertions.assertEquals("2020-12-11T08:17:32Z", sasMap.get("st"));
    }

    @Test
    void runtimeInfoAD() {
        final AdlsDatastoreRuntimeInfo info = this.buildRuntimeInfo(AuthMethod.ActiveDirectory);

        Assertions.assertTrue(info.getSASMap().isEmpty());
        final Map<String, String> adTokenMap = info.getAdTokenMap();
        Assertions.assertEquals("Bearer FakeToKen", adTokenMap.get(Constants.HeaderConstants.AUTHORIZATION));
    }

    @Test
    void runtimeInfoSharedKey() {
        final AdlsDatastoreRuntimeInfo info = this.buildRuntimeInfo(AuthMethod.SharedKey);

        Assertions.assertTrue(info.getAdTokenMap().isEmpty());
        Assertions.assertTrue(info.getSASMap().isEmpty());
    }

    private AdlsDatastoreRuntimeInfo buildRuntimeInfo(final AuthMethod authMethod) {
        final AdlsGen2Connection cnx = this.buildConnection();
        cnx.setAuthMethod(authMethod);
        return new AdlsDatastoreRuntimeInfo(cnx, new FakeActiveDirectoryService());
    }

    private AdlsGen2Connection buildConnection() {
        final AdlsGen2Connection cnx = new AdlsGen2Connection();
        cnx.setSas(
                "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2023-12-11T16:17:32Z&st=2020-12-11T08:17:32Z&spr=https&sig=xl5fq%2FA%2FCXA65xxxxxxxxxxs%2F%2Bjqqx6%2BSEepSXXX%3D");

        return cnx;
    }

}