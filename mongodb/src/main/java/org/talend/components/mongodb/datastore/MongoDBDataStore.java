package org.talend.components.mongodb.datastore;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        // @GridLayout.Row({ "Use_replica_set_address" }),
        @GridLayout.Row({ "Server" }), @GridLayout.Row({ "Port" }), @GridLayout.Row({ "Database" }),
        // @GridLayout.Row({ "Use_SSL" }),
        @GridLayout.Row({ "Authentication" }), @GridLayout.Row({ "Authentication_mechanism" }),
        @GridLayout.Row({ "Set_Authentication_Database" }), @GridLayout.Row({ "Authentication_Databse" }),
        @GridLayout.Row({ "Username" }), @GridLayout.Row({ "Password" }) })
@DataStore("Mongodb")
@Checkable("basic.healthcheck")
@Documentation("A connection to a Mongodb")
public class MongoDBDataStore implements Serializable {

    // @Option
    // @Documentation("Select this check box to show the Replica address table.")
    // private boolean Use_replica_set_address;

    @Option
    @Documentation("IP address")
    private String Server;

    @Option
    @Documentation("Listening port of the database server.")
    @DefaultValue("27017")
    @Pattern("\\d{0,5}")
    private String Port;

    @Option
    @Documentation("Name of the database.")
    private String Database;

    // @Option
    // @Documentation("Enable the SSL or TLS encrypted connection.")
    // private boolean Use_SSL;

    // @Option
    // @ActiveIf(target = "Use_SSL", value = "true")
    // @Documentation("TrustStroe type")
    // private trustStroe_type TrustStore_type;
    //
    // enum trustStroe_type {
    // JKS,
    // PCKS12
    // }
    //
    // @Option
    // @ActiveIf(target = "Use_SSL", value = "true")
    // @Credential
    // @Documentation("TrustStroe path")
    // private String TrustStore_path;
    //
    // @Option
    // @ActiveIf(target = "Use_SSL", value = "true")
    // @Documentation("TrustStroe password")
    // private String TrustStore_password;
    //
    // @Option
    // @ActiveIf(target = "Use_SSL", value = "true")
    // @Documentation("Enable the client auth.")
    // private boolean Need_client_auth;
    //
    // @Option
    // @ActiveIf(target = "Use_SSL", value = "true")
    // @Documentation("Enable the client auth.")
    // private boolean Verify_hostname;

    @Option
    @Documentation("Enable the database authentication.")
    private boolean Authentication;

    @Option
    @ActiveIf(target = "Authentication", value = "true")
    @Documentation("Among the mechanisms listed on the Authentication mechanism drop-down list, the NEGOTIATE one is recommended if you are not using Kerberos, because it automatically select the authentication mechanism the most adapted to the MongoDB version you are using.")
    private Authentication_method Authentication_mechanism;

    public enum Authentication_method {
        NEGOTIATE_MEC,
        PLAIN_MEC,
        SCRAMSHA1_MEC
        // ,KERBEROS_MEC
    }

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "Authentication", value = "true"),
            @ActiveIf(target = "Authentication_mechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC" }) })
    @Documentation("If the username to be used to connect to MongoDB has been created in a specific Authentication database of MongoDB, select this check box to enter the name of this Authentication database in the Authentication database field that is displayed.")
    private boolean Set_Authentication_Database;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "Authentication", value = "true"),
            @ActiveIf(target = "Set_Authentication_Database", value = "true"),
            @ActiveIf(target = "Authentication_mechanism", value = { "NEGOTIATE_MEC", "SCRAMSHA1_MEC" }) })
    @Documentation("Set MongoDB Authentication database")
    private String Authentication_Databse;

    @Option
    @ActiveIf(target = "Authentication", value = "true")
    @Documentation("Enter the username")
    private String Username;

    @Option
    @Credential
    @ActiveIf(target = "Authentication", value = "true")
    @Documentation("Enter the password")
    private String Password;

}
