package org.talend.components.onedrive.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.HealthChecker;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.messages.Messages;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

@Service
@Slf4j
public class OneDriveService {

    @Service
    private Messages i18n;

    @Service
    private HealthChecker healthChecker;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    /**
     * Return the schema that reflects Graph API DriveItem object.
     * Example of data for different types of DriveItem:
     *
     * root
     * {"@odata.context":"https://graph.microsoft.com/v1.0/$metadata#users('...')/drive/root/$entity"
     * ,"createdDateTime":"2018-08-16T16:53:00Z"
     * ,"id":"..."
     * ,"lastModifiedDateTime":"2018-10-05T09:23:41Z"
     * ,"name":"root"
     * ,"webUrl":"https://..."
     * ,"size":401687231
     * ,"parentReference":{"driveId":"...","driveType":"business"}
     * ,"fileSystemInfo":{"createdDateTime":"2018-08-16T16:53:00Z","lastModifiedDateTime":"2018-10-05T09:23:41Z"}
     * ,"folder":{"childCount":12}
     * ,"root":{}
     * }
     *
     * file
     * {"@microsoft.graph.downloadUrl":"https://..."
     * ,"createdDateTime":"2018-09-20T18:09:12Z"
     * ,"eTag":"\"{...},2\""
     * ,"id":"..."
     * ,"lastModifiedDateTime":"2018-09-20T18:13:24Z"
     * ,"name":"..."
     * ,"webUrl":"https://server/file"
     * ,"cTag":"\"c:{...},2\""
     * ,"size":113983488
     * ,"createdBy":{"user":{"email":"...","id":"...","displayName":"User"}}
     * ,"lastModifiedBy":{"user":{"email":"...,"id":"...","displayName":"User"}}
     * ,"parentReference":{"driveId":"...","driveType":"business","id":"...","path":"/drive/root:"}
     * ,"file":{"mimeType":"application/x-msdownload","hashes":{"quickXorHash":"zhNiD2Eh02XbWt2YP0WobQt3nOg="}}
     * ,"fileSystemInfo":{"createdDateTime":"2018-09-20T18:09:12Z","lastModifiedDateTime":"2018-09-20T18:13:24Z"}
     * }
     *
     * folder
     * {"createdDateTime":"2018-09-17T13:36:04Z"
     * ,"eTag":"\"{...},1\""
     * ,"id":"..."
     * ,"lastModifiedDateTime":"2018-09-17T13:36:04Z"
     * ,"name":"fold1"
     * ,"webUrl":"https://..."
     * ,"cTag":"\"c:{...},0\""
     * ,"size":46
     * ,"createdBy":{"user":{"email":"...","id":"...","displayName":"..."}}
     * ,"lastModifiedBy":{"user":{"email":"...","id":"...","displayName":"..."}}
     * ,"parentReference":{"driveId":"...","driveType":"business","id":"...","path":"/drive/root:"}
     * ,"fileSystemInfo":{"createdDateTime":"2018-09-17T13:36:04Z","lastModifiedDateTime":"2018-09-17T13:36:04Z"}
     * ,"folder":{"childCount":2}
     * }
     *
     * @return Schema
     */
    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID)
    public Schema guessTableSchemaList(final OneDriveDataSet dataSet) {
        Schema res = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build())
                .withEntry(
                        recordBuilderFactory.newEntryBuilder().withName("createdDateTime").withType(Schema.Type.STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("eTag").withType(Schema.Type.STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("lastModifiedDateTime").withType(Schema.Type.STRING)
                        .build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("name").withType(Schema.Type.STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("webUrl").withType(Schema.Type.STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("cTag").withType(Schema.Type.STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("size").withType(Schema.Type.LONG).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("createdBy").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("lastModifiedBy").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("parentReference").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("fileSystemInfo").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("folder").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("file").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("root").withType(Schema.Type.RECORD)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .build();
        return res;
    }

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_DELETE_ID)
    public Schema guessTableSchemaDelete(final OneDriveDataSet dataSet) {
        Schema res = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("id").withType(Schema.Type.STRING).build()).build();
        return res;
    }

    @HealthCheck(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
    public HealthCheckStatus validateBasicConnection(@Option final OneDriveDataStore dataStore) {
        try {
            log.debug("start health check");
            healthChecker.checkHealth(dataStore);
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.healthCheckOk());
    }

    @AsyncValidation("validateTenantId")
    public ValidationResult validateTenantId(String tenantId) {
        if (tenantId.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckTenantIdIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    @AsyncValidation("validateApplicationId")
    public ValidationResult validateApplicationId(String applicationId) {
        if (applicationId.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckApplicationIdIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    @AsyncValidation("validateAuthenticationLogin")
    public ValidationResult validateAuthenticationLogin(String authenticationLogin) {
        if (authenticationLogin.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckLoginIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }
}