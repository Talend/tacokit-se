package org.talend.components.workday.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.io.Serializable;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Slf4j
@Service
public class UIActionService implements Serializable {

    public static final String HEALTH_CHECK = "WORKDAY_HEALTH_CHECK";

    public static final String VALIDATION_URL_PROPERTY = "WORKDAY_VALIDATION_URL_PROPERTY";

    @Service
    private AccessTokenProvider service;

    @Service
    private I18n i18n;

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus validateConnection(@Option final WorkdayDataStore dataStore) {
        try {
            service.getAccessToken(dataStore);
        }
        catch (Exception e) {

            return new HealthCheckStatus(KO, i18n.healthCheckFailed("msg", e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}
