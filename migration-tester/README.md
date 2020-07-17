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

Some part has field xxx_from_yyyy. They try to be set by the yyyy handler since this field is xxx scope.
For example xxx='dso' and yyy=sink : the field dso_from_sink is part of datastore, and the SinkMigrationHandler will try to update it whereas it is not part of its scope. 

Each part has also a migration handler that:
- Copy legacy value in duplication field
- Update migration_handler_callback with a string built like "%from version% -> %to version% | yyyy/MM/dd HH:mm:ss"

The migrations handlers don't take care of the version and so you d'ont have to have two version of the connector.
The above updates are done everytime. 

The current version of the connector is available in org.talend.components.migration.conf.AbstractConfig.VERSION.

There should be always at least three migration handlers : on for the datastore, one for the dataset, on for the connector.
A proposal to have clean migration is that each migration handler should migrate only its scope.

Final applications have to well decide which migration handlers to call and when save migrated data.
For example, when we open a dataset form, if its associated connection need a migration, what is the desired behavior ?
We automatically call the migration handler of the connection and save it ?
Or we rely on an automatic migration at runtime, the migration will be saved only if we open /save the form of the migration ?

The source connector will return a single row which is the received configuration. So it is possible to understand which migration
handler has been called. Also, some newfield have been added :
- xxx_incoming : the configuration recieved by the xxx migration handler
- xxx_outgoing : the configuration after the migration

Since version 100 of the connector, a new @Required property has been added in the connection (datastore) : dso_shouldNotBeEmpty.
The connection migration handler set this property which is not set in previous version.
An excetion is throw by the TCK framework if this prperty is not part of the configuration : 
`- Property 'configuration.dse.dso.dso_shouldNotBeEmpty' is required.`

So for example, in the row returned by the source connector, the property `configuration.dse.dso.dso_shouldNotBeEmpty`
is not present in `configuration.dse.dso.dso_incoming` but should be in `configuration.dse.dso.dso_outgoing`. 