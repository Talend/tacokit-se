
To use Maven password decryption for integration testing add this settings to USERS_FOLDER\.m2\settings.xml file
<settings>
    <servers>
        <server>
            <id>onedrive</id>
            <username>USER</username>
            <password>{PASSWORD}</password>
        </server>
    </servers>
</settings>
