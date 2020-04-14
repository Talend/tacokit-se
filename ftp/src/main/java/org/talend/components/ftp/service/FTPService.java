/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.ftp.service;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;
import org.talend.components.ftp.dataset.FTPDataSet;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.service.ftpclient.FTPClientFactory;
import org.talend.components.ftp.service.ftpclient.GenericFTPClient;
import org.talend.components.ftp.service.ftpclient.GenericFTPFile;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class FTPService implements Serializable {

    public static final String ACTION_HEALTH_CHECK = "HEALTH_CHECK";

    public static final String PATH_SEPARATOR = "/";

    @Service
    private I18nMessage i18n;

    @Service
    private FTPClientFactory ftpClientFactory;

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus validateDataStore(@Option final FTPDataStore dataStore) {
        if (dataStore.getHost() == null || "".equals(dataStore.getHost().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.hostRequired());
        }

        try (GenericFTPClient ftpClient = getClient(dataStore)) {
            if (ftpClient.isConnected()) {
                return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
            } else {
                return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.statusNotConnected());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
    }

    public String generateFilePath(String dirPath, String filename) {
        return dirPath + (dirPath.endsWith(PATH_SEPARATOR) ? "" : PATH_SEPARATOR) + filename;
    }

    public GenericFTPClient getClient(FTPDataStore dataStore) {
        GenericFTPClient ftpClient = ftpClientFactory.getClient(dataStore);
        ftpClient.connect(dataStore.getHost(), dataStore.getPort());
        if (dataStore.isUseCredentials()) {
            if (!(ftpClient.auth(dataStore.getUsername(), dataStore.getPassword()))) {
                throw new FTPConnectorException(i18n.statusNotConnected());
            }
        }
        ftpClient.afterAuth(dataStore);
        return ftpClient;
    }

    /**
     * Checks if connection allows writting in a directory
     * 
     * @param dataset Dataset containing the path of the directory to test
     * @return true if user can write, false otherwise
     */
    public boolean canWrite(FTPDataSet dataset) {
        try (GenericFTPClient ftpClient = getClient(dataset.getDatastore())) {
            ftpClient.enableDebug(log);
            String filename = UUID.randomUUID().toString();
            String filePath = generateFilePath(dataset.getPath(), filename);
            OutputStream out = ftpClient.storeFileStream(filePath);
            if (out != null) {
                out.write("test".getBytes(StandardCharsets.UTF_8));
                out.flush();
                out.close();
                ftpClient.removeFile(filePath);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * Checks if the path in the dataset points to a single file
     * 
     * @param dataset
     * @return true if the path is a file, false otherwise
     */
    public boolean pathIsFile(FTPDataSet dataset) {
        try (GenericFTPClient ftpClient = getClient(dataset.getDatastore())) {
            List<GenericFTPFile> files = ftpClient.listFiles(dataset.getPath());
            String[] pathElements = dataset.getPath().split(PATH_SEPARATOR);
            String pathLastElement = pathElements[pathElements.length - 1];
            if (files.size() == 1) {
                return !files.get(0).isDirectory() && (files.get(0).getName().equals(pathLastElement));
            }

            return false;
        }

    }

}
