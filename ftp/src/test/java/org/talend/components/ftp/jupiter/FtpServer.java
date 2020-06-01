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
package org.talend.components.ftp.jupiter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.set.SynchronizedSet;
import org.apache.commons.net.ftp.FTPFile;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class FtpServer implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    public static final String USER = "ftpuser";

    public static final String PASSWD = "password";

    private FakeFtpServer server;

    private UnixFakeFileSystem fs;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {

        if (server != null && server.isStarted()) {
            log.debug("************************** Server STOP");
            server.stop();
            server = null;
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        Optional<FtpFile> ftpFile = AnnotationUtils.findAnnotation(extensionContext.getRequiredTestClass(), FtpFile.class);

        if (ftpFile.isPresent()) {
            if (server == null) {
                log.debug("************************** Server START");

                fs = new UnixFakeFileSystem();
                fs.setCreateParentDirectoriesAutomatically(true);
                URI baseUri = Thread.currentThread().getContextClassLoader().getResource(ftpFile.get().base() + "rootFTP")
                        .toURI();
                File ftpResourceDir = new File(baseUri).getParentFile();
                fs.add(new DirectoryEntry("/"));
                Arrays.stream(ftpResourceDir.listFiles()).filter(f -> !("rootFTP".equals(f.getName())))
                        .forEach(f -> addInFs(f, ""));

                server = new FakeFtpServer();
                server.addUserAccount(new UserAccount(USER, PASSWD, "/"));
                server.setFileSystem(fs);
                server.setServerControlPort(ftpFile.get().port());
                server.start();

                while (!server.isStarted()) {
                }
            }
        }
    }

    private void addInFs(File file, String base) {
        log.debug("Adding " + base + "/" + file.getName());
        if (file.isDirectory()) {
            fs.add(new DirectoryEntry(base + "/" + file.getName()));
            Arrays.stream(file.listFiles()).forEach(f -> addInFs(f, base + "/" + file.getName()));
        } else {
            FileEntry entry = new FileEntry(base + "/" + file.getName());
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try (FileInputStream in = new FileInputStream(file)) {
                byte[] buffer = new byte[1024];
                int read = 0;
                while ((read = in.read(buffer)) > 0) {
                    bout.write(buffer, 0, read);
                    bout.flush();
                }
            } catch (FileNotFoundException e) {
                log.error(e.getMessage());
            } catch (IOException e) {
                log.error(e.getMessage());
            }
            entry.setContents(bout.toByteArray());
            fs.add(entry);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(UnixFakeFileSystem.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.getParameter().getType().isAssignableFrom(UnixFakeFileSystem.class)) {
            return fs;
        }
        return null;
    }
}
