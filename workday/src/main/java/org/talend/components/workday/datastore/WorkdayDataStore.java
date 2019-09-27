package org.talend.components.workday.datastore;

import lombok.Data;
import lombok.ToString;
import org.talend.components.workday.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Base64;
import java.util.function.BiConsumer;

@Data
@DataStore(WorkdayDataStore.NAME)
@GridLayouts({ //
        @GridLayout({ //
                @GridLayout.Row({ "endpoint" }), //
                @GridLayout.Row({ "clientId", "clientSecret" }), //
                @GridLayout.Row({ "tenentAlias" }) //
        }) //
})
@Checkable(UIActionService.HEALTH_CHECK)
@Documentation(WorkdayDataStore.NAME)
@ToString
public class WorkdayDataStore implements Serializable {

    private static final long serialVersionUID = -8628647674176772061L;

    public static final String NAME = "WorkdayDataStore";

    @Option
    @Validable(UIActionService.VALIDATION_URL_PROPERTY)
    @Documentation("Workday token Auth Endpoint (host only, ie: https://auth.api.workday.com/v1/token)")
    private String endpoint;

    @Option
    @Documentation("Workday Client Id")
    private String clientId;

    @Option
    @Credential
    @Documentation("Workday Client Secret")
    private String clientSecret;

    @Option
    @Documentation("Workday tenant alias")
    private String tenantAlias;


    private transient Token token = null;

    public String getAuthorizationHeader() {
        final String idSecret = this.clientId + ':' + this.clientSecret;
        final String idForHeader = Base64.getEncoder().encodeToString(idSecret.getBytes());
        return "Basic " + idForHeader;
    }

    public void addAuthorizeHeader(BiConsumer<String, String> headerFunction) {
        headerFunction.accept("Authorization", getAuthorizationHeader());
    }

}
