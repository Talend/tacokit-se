/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.workday.service;

import org.talend.components.workday.datastore.WorkdayDataStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

public class ConfigHelper {

    public static Properties workdayProps() {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("workdayConfig.properties")) {
            Properties wkprops = new Properties();
            wkprops.load(in);
            return wkprops;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static WorkdayDataStore buildDataStore() {
        Properties props = ConfigHelper.workdayProps();
        WorkdayDataStore wds = new WorkdayDataStore();
        wds.setClientId(props.getProperty("clientId"));
        wds.setClientSecret(props.getProperty("clientSecret"));
        wds.setTenantAlias(props.getProperty("tenant"));
        wds.setAuthEndpoint(props.getProperty("authendpoint"));
        return wds;
    }
}
