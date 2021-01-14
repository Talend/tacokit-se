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
package org.talend.components.google.storage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface StorageFacade extends Serializable {

    /**
     * Build output to write on bucket/blob.
     * 
     * @param bucket : bucket to write.
     * @param blob : blob to write.
     * @return output stream correspond to bucket/blob.
     */
    OutputStream buildOuput(final String bucket, final String blob);

    /**
     * Build input stream getter on bucket/blob
     * 
     * @param bucket : bucket.
     * @param blob : blob.
     * @return input stream getter to read data.
     */
    Supplier<InputStream> buildInput(final String bucket, final String blob);

    /**
     * Find all blob for a given bucket that match name.
     * 
     * @param bucket : bucket name.
     * @param blobStartName : start name of blob so if start name is MyBlob, then MyBlobA will match.
     * @return all blob name that matches.
     */
    Stream<String> findBlobsName(final String bucket, final String blobStartName);

    /**
     * Check if a bucket exist.
     * 
     * @param bucketName : bucket name.
     * @return true if exist.
     */
    boolean isBucketExist(final String bucketName);

    /**
     * Check if a blob exist.
     *
     * @param bucketName : bucket name.
     * @param blobName : blob name.
     * @return true if exist.
     */
    boolean isBlobExist(final String bucketName, final String blobName);
}
