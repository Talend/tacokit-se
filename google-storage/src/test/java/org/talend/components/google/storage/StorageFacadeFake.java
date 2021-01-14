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
package org.talend.components.google.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.talend.components.google.storage.service.StorageFacade;

import lombok.Getter;

public class StorageFacadeFake implements StorageFacade {

    private static class Bucket {

        @Getter
        private final String name;

        private final File root;

        private final SortedMap<String, File> blobs = new TreeMap<>();

        public Bucket(String name, File folder) {
            this.name = name;
            this.root = folder;
            final File[] files = folder.listFiles();
            if (files != null) {
                for (File f : files) {
                    this.blobs.put(f.getName(), f);
                }
            }
        }

        public File getBlob(String name) {
            return this.blobs.get(name);
        }

        public File buildBlob(String name) {
            final File f = new File(root, name);
            this.blobs.put(name, f);
            if (!f.exists()) {
                try {
                    f.createNewFile();
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            return f;
        }

        public Collection<String> list(String startName) {
            return this.blobs.subMap(startName, startName + Character.MAX_VALUE).keySet();
        }
    }

    private final Bucket bucket;

    public StorageFacadeFake(String name, File folder) {
        this.bucket = new Bucket(name, folder);
    }

    @Override
    public OutputStream buildOuput(final String bucket, final String blob) {
        if (Objects.equals(this.bucket.getName(), bucket)) {
            final File f = this.bucket.buildBlob(blob);
            try {
                return new FileOutputStream(f);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        return null;
    }

    @Override
    public Supplier<InputStream> buildInput(String bucket, String blob) {
        if (Objects.equals(this.bucket.getName(), bucket)) {
            final File file = this.bucket.getBlob(blob);
            if (file != null) {
                return () -> {
                    try {
                        return new FileInputStream(file);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                };
            }
        }
        return null;
    }

    @Override
    public Stream<String> findBlobsName(String bucket, String blobStartName) {
        if (Objects.equals(this.bucket.getName(), bucket)) {
            return this.bucket.list(blobStartName).stream();
        }
        return null;
    }

    @Override
    public boolean isBucketExist(String bucketName) {
        return Objects.equals(this.bucket.name, bucketName);
    }

    @Override
    public boolean isBlobExist(String bucketName, String blobName) {
        final Stream<String> names = this.findBlobsName(bucketName, blobName);
        return names != null && names.count() > 0;
    }
}
