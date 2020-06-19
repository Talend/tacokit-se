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
package org.talend.components.ftp.source;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.ftp.service.FTPService;
import org.talend.components.ftp.service.I18nMessage;
import org.talend.components.ftp.service.ftpclient.GenericFTPClient;
import org.talend.components.ftp.service.ftpclient.GenericFTPFile;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "ftp")
@PartitionMapper(name = "FTPInput")
@Documentation("This component reads a FTP folder.")
@Slf4j
@RequiredArgsConstructor
public class FTPPartitionMapper implements Serializable {

    protected final FTPInputConfiguration configuration;

    protected final FTPService ftpService;

    protected final RecordBuilderFactory recordBuilderFactory;

    protected final RecordIORepository recordIORepository;

    protected final I18nMessage i18n;

    protected List<GenericFTPFile> filesToRead;

    @Assessor
    public long estimateSize() {
        try (GenericFTPClient ftpClient = ftpService.getClient(configuration)) {
            return ftpClient.listFiles(configuration.getDataSet().getPath()).stream().mapToLong(GenericFTPFile::getSize).sum();

        }
    }

    public long computeSizeToRead() {
        if (filesToRead == null) {
            return 0;
        }

        return filesToRead.stream().mapToLong(GenericFTPFile::getSize).sum();
    }

    private FTPPartitionMapper addNewMapper(List<FTPPartitionMapper> mappers) {
        FTPPartitionMapper newMapper = new FTPPartitionMapper(configuration, ftpService, recordBuilderFactory, recordIORepository,
                i18n);
        mappers.add(newMapper);
        return newMapper;
    }

    protected void addFileToRead(GenericFTPFile file) {
        if (filesToRead == null) {
            filesToRead = new ArrayList<>();
        }

        filesToRead.add(file);
    }

    @Split
    public List<FTPPartitionMapper> split(@PartitionSize final long bundleSize) {

        List<FTPPartitionMapper> mappers = new ArrayList<>();
        List<GenericFTPFile> filesToRead = null;
        try (GenericFTPClient ftpClient = ftpService.getClient(configuration)) {
            filesToRead = ftpClient.listFiles(configuration.getDataSet().getPath());
        }

        if (filesToRead != null) {
            filesToRead.sort(new GenericFTPFile.GenericFTPFileSizeComparator());
            filesToRead.stream().forEach(file -> {
                long fileSize = file.getSize();
                mappers.stream().filter(m -> m.computeSizeToRead() + fileSize <= bundleSize).findFirst()
                        .orElseGet(() -> addNewMapper(mappers)).addFileToRead(file);
            });
        }

        log.debug(mappers.toString());

        return mappers;
    }

    @Emitter
    public FTPInput createSource() {
        return new FTPInput(configuration, ftpService, recordBuilderFactory, i18n, recordIORepository, filesToRead);
    }

}
