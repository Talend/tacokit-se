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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.output.FTPOutputConfiguration;
import org.talend.components.ftp.service.FTPService;
import org.talend.components.ftp.service.I18nMessage;
import org.talend.components.ftp.source.FTPInputConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Predicate;

public abstract class GenericFTPClient implements AutoCloseable {

    @Setter
    @Getter(AccessLevel.PROTECTED)
    private I18nMessage i18n;

    public abstract void connect(String host, int port);

    public abstract boolean auth(String username, String password);

    public abstract void afterAuth(FTPDataStore dataStore);

    public abstract boolean isConnected();

    public abstract void disconnect();

    public abstract void enableDebug(Logger log);

    public abstract List<GenericFTPFile> listFiles(String path, Predicate<GenericFTPFile> filter);

    public final List<GenericFTPFile> listFiles(String path) {
        return listFiles(path, null);
    }

    public abstract void retrieveFile(String path, OutputStream out);

    public abstract void configure(FTPInputConfiguration configuration);

    public abstract void configure(FTPOutputConfiguration configuration);

    public abstract boolean storeFile(String path, InputStream stream);

    public abstract OutputStream storeFileStream(String path);

    public abstract String getReplyCode();

    @Override
    public final void close() {
        disconnect();
    }

    public abstract void removeFile(String filePath);

    /**
     * Extract filename from fullpath
     * 
     * @param fullpath
     * @return
     */
    protected final String getFilename(String fullpath) {
        String[] pathElements = fullpath.split(FTPService.PATH_SEPARATOR);
        if (pathElements.length > 0) {
            return pathElements[pathElements.length - 1];
        }
        return fullpath;
    }
}
