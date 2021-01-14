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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.BucketAccessControl;
import com.google.api.services.storage.model.HmacKey;
import com.google.api.services.storage.model.HmacKeyMetadata;
import com.google.api.services.storage.model.Notification;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.Policy;
import com.google.api.services.storage.model.ServiceAccount;
import com.google.api.services.storage.model.StorageObject;
import com.google.api.services.storage.model.TestIamPermissionsResponse;
import com.google.cloud.Tuple;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.spi.v1.RpcBatch;
import com.google.cloud.storage.spi.v1.StorageRpc;

public class FakeStorage implements StorageRpc {

    private final StorageRpc storageRpc;

    public FakeStorage(StorageRpc storageRpc) {
        this.storageRpc = storageRpc;
    }

    public static Storage buildForTU() {
        final StorageOptions options = LocalStorageHelper.getOptions();
        final StorageRpc rpc = (StorageRpc) options.getRpc();
        final StorageRpc fake = new FakeStorage(rpc);

        return LocalStorageHelper.getOptions().toBuilder().setServiceRpcFactory((StorageOptions so) -> fake).build().getService();
    }

    public Bucket create(Bucket bucket, Map<Option, ?> options) {
        return storageRpc.create(bucket, options);
    }

    public StorageObject create(StorageObject object, InputStream content, Map<Option, ?> options) {
        return storageRpc.create(object, content, options);
    }

    public Tuple<String, Iterable<Bucket>> list(Map<Option, ?> options) {
        return storageRpc.list(options);
    }

    public Tuple<String, Iterable<StorageObject>> list(String bucket, Map<Option, ?> options) {
        return storageRpc.list(bucket, options);
    }

    public Bucket get(Bucket bucket, Map<Option, ?> options) {
        return storageRpc.get(bucket, options);
    }

    public StorageObject get(StorageObject object, Map<Option, ?> options) {
        return storageRpc.get(object, options);
    }

    public Bucket patch(Bucket bucket, Map<Option, ?> options) {
        return storageRpc.patch(bucket, options);
    }

    public StorageObject patch(StorageObject storageObject, Map<Option, ?> options) {
        return storageRpc.patch(storageObject, options);
    }

    public boolean delete(Bucket bucket, Map<Option, ?> options) {
        return storageRpc.delete(bucket, options);
    }

    public boolean delete(StorageObject object, Map<Option, ?> options) {
        return storageRpc.delete(object, options);
    }

    public RpcBatch createBatch() {
        return storageRpc.createBatch();
    }

    public StorageObject compose(Iterable<StorageObject> sources, StorageObject target, Map<Option, ?> targetOptions) {
        return storageRpc.compose(sources, target, targetOptions);
    }

    public byte[] load(StorageObject storageObject, Map<Option, ?> options) {
        return storageRpc.load(storageObject, options);
    }

    public Tuple<String, byte[]> read(StorageObject from, Map<Option, ?> options, long position, int bytes) {
        return storageRpc.read(from, options, position, bytes);
    }

    public long read(StorageObject from, Map<Option, ?> options, long position, OutputStream outputStream) {
        return storageRpc.read(from, options, position, outputStream);
    }

    public String open(StorageObject object, Map<Option, ?> options) {
        return storageRpc.open(object, options);
    }

    public String open(String signedURL) {
        return storageRpc.open(signedURL);
    }

    public void write(String uploadId, byte[] toWrite, int toWriteOffset, long destOffset, int length, boolean last) {
        storageRpc.write(uploadId, toWrite, toWriteOffset, destOffset, length, last);
    }

    public StorageObject writeWithResponse(String uploadId, byte[] toWrite, int toWriteOffset, long destOffset, int length,
            boolean last) {
        storageRpc.write(uploadId, toWrite, toWriteOffset, destOffset, length, last);
        return null;
    }

    public RewriteResponse openRewrite(RewriteRequest rewriteRequest) {
        return storageRpc.openRewrite(rewriteRequest);
    }

    public RewriteResponse continueRewrite(RewriteResponse previousResponse) {
        return storageRpc.continueRewrite(previousResponse);
    }

    public BucketAccessControl getAcl(String bucket, String entity, Map<Option, ?> options) {
        return storageRpc.getAcl(bucket, entity, options);
    }

    public boolean deleteAcl(String bucket, String entity, Map<Option, ?> options) {
        return storageRpc.deleteAcl(bucket, entity, options);
    }

    public BucketAccessControl createAcl(BucketAccessControl acl, Map<Option, ?> options) {
        return storageRpc.createAcl(acl, options);
    }

    public BucketAccessControl patchAcl(BucketAccessControl acl, Map<Option, ?> options) {
        return storageRpc.patchAcl(acl, options);
    }

    public List<BucketAccessControl> listAcls(String bucket, Map<Option, ?> options) {
        return storageRpc.listAcls(bucket, options);
    }

    public ObjectAccessControl getDefaultAcl(String bucket, String entity) {
        return storageRpc.getDefaultAcl(bucket, entity);
    }

    public boolean deleteDefaultAcl(String bucket, String entity) {
        return storageRpc.deleteDefaultAcl(bucket, entity);
    }

    public ObjectAccessControl createDefaultAcl(ObjectAccessControl acl) {
        return storageRpc.createDefaultAcl(acl);
    }

    public ObjectAccessControl patchDefaultAcl(ObjectAccessControl acl) {
        return storageRpc.patchDefaultAcl(acl);
    }

    public List<ObjectAccessControl> listDefaultAcls(String bucket) {
        return storageRpc.listDefaultAcls(bucket);
    }

    public ObjectAccessControl getAcl(String bucket, String object, Long generation, String entity) {
        return storageRpc.getAcl(bucket, object, generation, entity);
    }

    public boolean deleteAcl(String bucket, String object, Long generation, String entity) {
        return storageRpc.deleteAcl(bucket, object, generation, entity);
    }

    public ObjectAccessControl createAcl(ObjectAccessControl acl) {
        return storageRpc.createAcl(acl);
    }

    public ObjectAccessControl patchAcl(ObjectAccessControl acl) {
        return storageRpc.patchAcl(acl);
    }

    public List<ObjectAccessControl> listAcls(String bucket, String object, Long generation) {
        return storageRpc.listAcls(bucket, object, generation);
    }

    public HmacKey createHmacKey(String serviceAccountEmail, Map<Option, ?> options) {
        return storageRpc.createHmacKey(serviceAccountEmail, options);
    }

    public Tuple<String, Iterable<HmacKeyMetadata>> listHmacKeys(Map<Option, ?> options) {
        return storageRpc.listHmacKeys(options);
    }

    public HmacKeyMetadata updateHmacKey(HmacKeyMetadata hmacKeyMetadata, Map<Option, ?> options) {
        return storageRpc.updateHmacKey(hmacKeyMetadata, options);
    }

    public HmacKeyMetadata getHmacKey(String accessId, Map<Option, ?> options) {
        return storageRpc.getHmacKey(accessId, options);
    }

    public void deleteHmacKey(HmacKeyMetadata hmacKeyMetadata, Map<Option, ?> options) {
        storageRpc.deleteHmacKey(hmacKeyMetadata, options);
    }

    public Policy getIamPolicy(String bucket, Map<Option, ?> options) {
        return storageRpc.getIamPolicy(bucket, options);
    }

    public Policy setIamPolicy(String bucket, Policy policy, Map<Option, ?> options) {
        return storageRpc.setIamPolicy(bucket, policy, options);
    }

    public TestIamPermissionsResponse testIamPermissions(String bucket, List<String> permissions, Map<Option, ?> options) {
        return storageRpc.testIamPermissions(bucket, permissions, options);
    }

    public boolean deleteNotification(String bucket, String notification) {
        return storageRpc.deleteNotification(bucket, notification);
    }

    public List<Notification> listNotifications(String bucket) {
        return storageRpc.listNotifications(bucket);
    }

    public Notification createNotification(String bucket, Notification notification) {
        return storageRpc.createNotification(bucket, notification);
    }

    public Bucket lockRetentionPolicy(Bucket bucket, Map<Option, ?> options) {
        return storageRpc.lockRetentionPolicy(bucket, options);
    }

    public ServiceAccount getServiceAccount(String projectId) {
        return storageRpc.getServiceAccount(projectId);
    }

}
