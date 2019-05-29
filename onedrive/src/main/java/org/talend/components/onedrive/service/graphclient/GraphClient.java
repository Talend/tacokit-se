package org.talend.components.onedrive.service.graphclient;

import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.logger.ILogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.AuthorizationHelper;

@Slf4j
public class GraphClient {

    @Getter
    private IGraphServiceClient graphServiceClient;

    @Setter
    private String accessToken;

    @Getter
    private DriveItem root;

    public GraphClient(OneDriveDataStore dataStore, AuthorizationHelper authorizationHelper) {
        IAuthenticationProvider authenticationProvider = request -> {
            request.addHeader("Authorization", accessToken);
        };

        graphServiceClient = GraphServiceClient.builder().authenticationProvider(authenticationProvider).logger(getLogger())
                .buildClient();
        setAccessToken(authorizationHelper.getAuthorization(dataStore));
        root = getDriveRequestBuilder().root().buildRequest().get();
    }

    public IDriveRequestBuilder getDriveRequestBuilder() {
        return graphServiceClient.me().drive();
    }

    /*
     * return dummy implementation to avoid logging every instruction (like in default implementation)
     */
    private ILogger getLogger() {
        return new ILogger() {

            @Override
            public void setLoggingLevel(LoggerLevel loggerLevel) {

            }

            @Override
            public LoggerLevel getLoggingLevel() {
                return null;
            }

            @Override
            public void logDebug(String s) {
                log.debug(s);
            }

            @Override
            public void logError(String s, Throwable throwable) {
                log.error(s);
            }
        };
    }
}
