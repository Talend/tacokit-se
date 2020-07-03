**Migration tester**

This module is dedicated to test tck migrations integration in final application. It provides :
- A datastore
- A dataset
- A source with its own configuration class
- A sink with its own configuration class

Each part has the same properties structure:
- legacy
- duplication
- migration_handler_callback

Each part has also a migration handler that:
- Copy legacy value in duplication field
- Update migration_handler_callback with a string built like "%from version% -> %to version% | yyyy/MM/dd HH:mm:ss"

The current version of the connector is available in org.talend.components.migration.conf.AbstractConfig.VERSION.

There should be always at least three migration handlers : on for the datastore, one for the dataset, on for the connector.
A proposal to have clean migration is that each migration handler should migrate only its scope.

Final applications have to well decide which migration handlers to call and when save migrated data.
For example, when we open a dataset form, if its associated connection need a migration, what is the desired behavior ?
We automatically call the migration handler of the connection and save it ?
Or we rely on an automatic migration at runtime, the migration will be saved only if we open /save the form of the migration ? 