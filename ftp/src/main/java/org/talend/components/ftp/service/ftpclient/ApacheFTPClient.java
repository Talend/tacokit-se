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
package org.talend.components.ftp.service.ftpclient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;
import org.slf4j.Logger;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.output.FTPOutputConfiguration;
import org.talend.components.ftp.source.FTPInputConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
/**
 * FTP client for FTP and FTPS
 */
public class ApacheFTPClient extends GenericFTPClient {

    public static final int DEFAULT_FTP_PORT = 21;

    public static final int DEFAULT_FTPS_PORT = 990;

    private FTPClient ftpClient;

    private boolean isFTPS;

    private ApacheFTPClient() {

    }

    public static ApacheFTPClient createFTP(FTPDataStore dataStore) {
        FTPClient ftpClient = ftpClient = new FTPClient();

        ftpClient.setControlKeepAliveTimeout(dataStore.getKeepAliveTimeout());
        ftpClient.setControlKeepAliveReplyTimeout(dataStore.getKeepAliveReplyTimeout());

        ApacheFTPClient result = new ApacheFTPClient();
        result.ftpClient = ftpClient;

        return result;
    }

    public static ApacheFTPClient createFTPS(FTPDataStore dataStore) {

        FTPSClient ftps = new FTPSClient(dataStore.getProtocol(), dataStore.getPort() <= 0);
        switch (dataStore.getTrustType()) {
        case ALL:
            ftps.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());
            break;
        case VALID:
            ftps.setTrustManager(TrustManagerUtils.getValidateServerCertificateTrustManager());
            break;
        case NONE:
            ftps.setTrustManager(null);
            break;
        default:
            throw new UnsupportedOperationException("Unsupported trust type: " + dataStore.getTrustType());
        }
        FTPClient ftpClient = ftps;

        ftpClient.setControlKeepAliveTimeout(dataStore.getKeepAliveTimeout());
        ftpClient.setControlKeepAliveReplyTimeout(dataStore.getKeepAliveReplyTimeout());

        ApacheFTPClient result = new ApacheFTPClient();
        result.ftpClient = ftpClient;
        result.isFTPS = true;

        return result;
    }

    @Override
    public void connect(String host, int port) {
        port = port <= 0 ? (isFTPS ? DEFAULT_FTPS_PORT : DEFAULT_FTP_PORT) : port;
        try {
            ftpClient.connect(host, port);
        } catch (Exception e) {
            log.error(getI18n().errorConnection(e.getMessage()), e);
        }
    }

    @Override
    public boolean auth(String username, String password) {
        try {
            return ftpClient.login(username, password);
        } catch (IOException ioe) {
            log.error(getI18n().errorConnection(ioe.getMessage()));
            return false;
        }
    }

    @Override
    public void afterAuth(FTPDataStore dataStore) {
        ftpClient.enterLocalPassiveMode();
    }

    @Override
    public boolean isConnected() {
        return ftpClient != null && ftpClient.isConnected();
    }

    @Override
    public void disconnect() {
        if (ftpClient != null) {
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                log.warn(getI18n().warnCannotDisconnect(e.getMessage()));
            }
        }
        ftpClient = null;
    }

    @Override
    public void enableDebug(Logger alog) {
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(new LogWriter(alog)), true));
    }

    @Override
    public List<GenericFTPFile> listFiles(String path, Predicate<GenericFTPFile> filter) {
        try {
            FTPFile[] files = ftpClient.listFiles(path);
            return Arrays.stream(files).map(this::toGenericFTPFile).filter(f -> filter == null ? true : filter.test(f))
                    .collect(Collectors.toList());
        } catch (IOException ioe) {
            log.error(getI18n().errorListFiles(ioe.getMessage()));
            return null;
        }
    }

    private <R> GenericFTPFile toGenericFTPFile(FTPFile ftpFile) {
        GenericFTPFile genericFTPFile = new GenericFTPFile();

        genericFTPFile.setName(getFilename(ftpFile.getName()));
        genericFTPFile.setDirectory(ftpFile.isDirectory());
        genericFTPFile.setSize(ftpFile.getSize());

        return genericFTPFile;
    }

    @Override
    public void retrieveFile(String path, OutputStream out) {
        try {
            ftpClient.retrieveFile(path, out);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void configure(FTPInputConfiguration configuration) {

    }

    @Override
    public void configure(FTPOutputConfiguration configuration) {

    }

    @Override
    public boolean storeFile(String path, InputStream stream) {
        try {
            return ftpClient.storeFile(path, stream);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public OutputStream storeFileStream(String path) {
        try {
            return ftpClient.storeFileStream(path);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String getReplyCode() {
        try {
            return ftpClient.getReply() + " : " + ftpClient.getReplyString();
        } catch (IOException e) {
            log.error(e.getMessage());
            return e.getMessage();
        }
    }

    @Override
    public void removeFile(String filePath) {
        try {
            ftpClient.deleteFile(filePath);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

}
