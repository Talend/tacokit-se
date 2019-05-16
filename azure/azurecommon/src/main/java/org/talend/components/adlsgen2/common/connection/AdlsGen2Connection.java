package org.talend.components.adlsgen2.common.connection;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.components.adlsgen2.common.service.ADLSGen2Service.ACTION_SUGGESTION_FILESYSTEMS;

@Version(1)
@Data
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row("accountName"), @GridLayout.Row("accountKey"), @GridLayout.Row("fileSystem") })
public class AdlsGen2Connection implements Serializable {

    @Option
    @Required
    @Documentation("Name of the storage account you need to access. "
            + "A storage account name can be found in the Storage accounts dashboard of the Microsoft Azure Storage system to be used. "
            + "Ensure that the administrator of the system has granted you the appropriate access permissions to this storage account.")
    private String accountName;

    @Option
    @Required
    @Documentation("Storage Shared Key")
    @Credential
    private String accountKey;

    @Option
    @Documentation("fileSystem")
    @Suggestable(value = ACTION_SUGGESTION_FILESYSTEMS, parameters = { "accountName", "accountKey" })
    private String fileSystem;

    // @Option
    // @Required
    // @Documentation("Path to Blob Object")
    // @Suggestable(value = ACTION_SUGGESTION_PATHS, parameters = { "accountName", "accountKey","fileSystem","blobPath" })
    // private String blobPath = "tempFolder" + nanoTime();

    public String apiUrl() {
        return String.format(Constants.DFS_URL, getAccountName());
        // return String.format("https://%s.blob.core.windows.net", getAccountName());
    }

}
