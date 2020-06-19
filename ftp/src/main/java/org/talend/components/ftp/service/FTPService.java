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

import lombok.extern.slf4j.Slf4j;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.service.ftpclient.FTPClientFactory;
import org.talend.components.ftp.service.ftpclient.GenericFTPClient;
import org.talend.components.ftp.service.ftpclient.GenericFTPFile;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FTPService implements Serializable {

    public static final String ACTION_HEALTH_CHECK = "HEALTH_CHECK";

    public static final String PATH_SEPARATOR = "/";

    public static final String ACTION_SUGGESTION_PATH = "SUGGESTION_PATH";

    @Service
    private I18nMessage i18n;

    @Service
    private FTPClientFactory ftpClientFactory;

    private transient Map<String, GenericFTPClient> clientMap;

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

    @Suggestions(ACTION_SUGGESTION_PATH)
    public SuggestionValues suggestPath(@Option final FTPDataStore datastore, @Option final String path) {
        try (GenericFTPClient ftpClient = getClient(datastore)) {
            return new SuggestionValues(false,
                    ftpClient.listFiles(getDirectory(path)).stream().map(f -> generateFilePath(path, f.getName()))
                            .map(p -> new SuggestionValues.Item(p, p)).collect(Collectors.toList()));
        }

    }

    public boolean hasWritePermission(FTPConnectorConfiguration configuration) {
        String path = configuration.getDataSet().getPath();
        if (path.endsWith(PATH_SEPARATOR)) {
            path = path.substring(0, path.length() - PATH_SEPARATOR.length());
        }
        String[] pathElements = path.split(PATH_SEPARATOR);
        String pathLastElement = pathElements[pathElements.length - 1];
        try (GenericFTPClient client = getClient(configuration)) {
            return client.listFiles(getDirectory(path)).stream().filter(GenericFTPFile::isDirectory)
                    .filter(f -> f.getName().equals(pathLastElement)).map(GenericFTPFile::isWritable).findFirst().orElse(false)
                    .booleanValue();

        }
    }

    private String getDirectory(String path) {
        if (path == null || "".equals(path.trim())) {
            return "/";
        }

        return path.substring(0, path.lastIndexOf('/'));
    }

    public String generateFilePath(String dirPath, String filename) {
        if (dirPath == null) {
            dirPath = "";
        }
        return dirPath + (dirPath.endsWith(PATH_SEPARATOR) ? "" : PATH_SEPARATOR) + filename;
    }

    public GenericFTPClient getClient(final FTPConnectorConfiguration configuration) {
        if (clientMap == null) {
            clientMap = new ConcurrentHashMap<>();
        }

        return clientMap.compute(configuration.getConfigKey(), (key, current) -> {
            if (current == null || !current.isConnected()) {
                return getClient(configuration.getDataSet().getDatastore());
            }
            return current;
        });
    }

    protected GenericFTPClient getClient(FTPDataStore dataStore) {
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

    public void releaseClient(FTPConnectorConfiguration configuration) {
        GenericFTPClient client = clientMap.remove(configuration.getConfigKey());
        if (client != null) {
            client.disconnect();
            client.close();
        }
    }

    /**
     * Checks if the path points to a single file
     * 
     * @param ftpClient the FTP client to use
     * @param path the path to test
     * @return true if the path is a file, false otherwise
     */
    public boolean pathIsFile(GenericFTPClient ftpClient, String path) {
        List<GenericFTPFile> files = ftpClient.listFiles(path);
        String[] pathElements = path.split(PATH_SEPARATOR);
        String pathLastElement = pathElements[pathElements.length - 1];
        if (files.size() == 1) {
            return !files.get(0).isDirectory() && (files.get(0).getName().equals(pathLastElement));
        }

        return false;
    }

}
