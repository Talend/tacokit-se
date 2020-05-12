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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.output.FTPOutputConfiguration;
import org.talend.components.ftp.source.FTPInputConfiguration;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Vector;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
/**
 * FTP client for SFTP connection
 */
public class JschFTPSClient extends GenericFTPClient {

    public static final int DEFAULT_SFTP_PORT = 22;

    public static class LoggerWrapper implements com.jcraft.jsch.Logger {

        @Setter
        private Logger log;

        @Override
        public boolean isEnabled(int level) {
            if (log == null) {
                return false;
            }

            switch (level) {
            case com.jcraft.jsch.Logger.DEBUG:
                return log.isDebugEnabled();
            case com.jcraft.jsch.Logger.INFO:
                return log.isInfoEnabled();
            case com.jcraft.jsch.Logger.WARN:
                return log.isWarnEnabled();
            case com.jcraft.jsch.Logger.ERROR:
                return log.isErrorEnabled();
            default:
                return true;
            }
        }

        @Override
        public void log(int level, String s) {
            if (log != null) {
                switch (level) {
                case com.jcraft.jsch.Logger.DEBUG:
                    log.debug(s);
                case com.jcraft.jsch.Logger.INFO:
                    log.info(s);
                case com.jcraft.jsch.Logger.WARN:
                    log.warn(s);
                case com.jcraft.jsch.Logger.ERROR:
                case com.jcraft.jsch.Logger.FATAL:
                    log.error(s);
                }
            }
        }
    }

    private Session session;

    private ChannelSftp channel;

    private String host;

    private int port;

    private LoggerWrapper logger;

    private JschFTPSClient() {

    }

    public static JschFTPSClient create(FTPDataStore dataStore) {

        JschFTPSClient client = new JschFTPSClient();

        return client;
    }

    @Override
    public void connect(String host, int port) {
        this.host = host;
        this.port = port <= 0 ? DEFAULT_SFTP_PORT : port;
    }

    @Override
    public boolean auth(String username, String password) {
        JSch jsch = new JSch();
        try {
            logger = new LoggerWrapper();
            jsch.setLogger(logger);
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            return true;
        } catch (JSchException e) {
            log.error(getI18n().errorConnection(e.getMessage()), e);
            return false;
        }

    }

    @Override
    public void afterAuth(FTPDataStore dataStore) {

    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }

        channel = null;
        session = null;
    }

    @Override
    public void enableDebug(Logger log) {
        logger.setLog(log);
    }

    @Override
    public List<GenericFTPFile> listFiles(String path, Predicate<GenericFTPFile> filter) {
        try {
            Vector<ChannelSftp.LsEntry> entries = channel.ls(path);
            return entries.stream().map(this::toGenericFTPFile).filter(f -> filter == null ? true : filter.test(f))
                    .collect(Collectors.toList());
        } catch (SftpException e) {
            getI18n().errorListFiles(e.getMessage());
            return null;
        }
    }

    private GenericFTPFile toGenericFTPFile(ChannelSftp.LsEntry lsEntry) {
        GenericFTPFile ftpFile = new GenericFTPFile();

        ftpFile.setName(lsEntry.getFilename());
        ftpFile.setSize(lsEntry.getAttrs().getSize());
        ftpFile.setDirectory(lsEntry.getAttrs().isDir());

        return ftpFile;
    }

    @Override
    public void retrieveFile(String path, OutputStream out) {
        try {
            channel.get(path, out);
        } catch (SftpException e) {
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
            channel.put(stream, path);
            return true;
        } catch (SftpException e) {
            log.error(e.getMessage(), e);
            return false;
        }

    }

    @Override
    public OutputStream storeFileStream(String path) {
        try {
            return channel.put(path);
        } catch (SftpException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String getReplyCode() {
        return "???";
    }

    @Override
    public void removeFile(String filePath) {
        try {
            channel.rm(filePath);
        } catch (SftpException e) {
            log.error(e.getMessage(), e);
        }
    }
}
