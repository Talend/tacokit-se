# Migration tester

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

## Matrice attributes explanation

|                       	| Prefix  	| legacy                                                                                                                   	| duplication                                                	| incoming                                                                              	| outgoing                                                                                                 	| migration_handler_callback                                                                                                                                                                                    	| from_sink                                                             	| from_source                                                              	| from_dataset                                                               	|
|-----------------------	|---------	|--------------------------------------------------------------------------------------------------------------------------	|------------------------------------------------------------	|---------------------------------------------------------------------------------------	|----------------------------------------------------------------------------------------------------------	|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|-----------------------------------------------------------------------	|--------------------------------------------------------------------------	|----------------------------------------------------------------------------	|
| **Datastore/Connection**  | dso_    	| Input a value in it from the new connection Form                                                                         	| The datastore migration handler copy Legacy in this field. 	| The migration handler will store in this Field the input configuration                	| The migration handler will store the migrated configuration in this field (without this field of course) 	| The migration handler will set A string formatted the incoming version, the new one, and the date “1 → 2 \| date”                                                                                             	| A value set by the sink migration handler in the datastore/connection 	| A value set by the source migration handler in the datastore/connection  	| A value set by the dataset migration handler in the datastore/connection   	|
| **Dataset**               | dse_    	| Input a value in it from the new dataset form. Select the dataset create above.                                          	| The dataset migration handler copy Legacy in this field.   	| The migration handler will store in this Field the input configuration                	| The migration handler will store the migrated configuration in this field (without this field of course) 	| The migration handler will set A string formatted the incoming version, the new one, and the date “1 → 2 \| date”                                                                                             	| A value set by the sink migration handler in the Dataset              	| A value set by the source Migration handler in the dataset               	| x                                                                          	|
| **Source**                | source_ 	| Create a new pipeline, set the above dataset as source. In the connector configuration panel Set a value in this field   	| The source migration handler copy Legacy in this field.    	| The migration handler will store in this Field the input configuration                	| The migration handler will store the migrated configuration in this field (without this field of course) 	| The migration handler will set A string formatted the incoming version, the new one, and the date “1 → 2 \| date”                                                                                             	| x                                                                     	| x                                                                        	| x                                                                          	|
| **Sink**                  | sink_   	| In the new pipeline, select the above dataset As output. Set a value in this field in the connector configuration panel. 	| The sink migration handler copy Legacy in this field.      	| The migration handler will store in this Field the input configuration                	| The migration handler will store the migrated configuration in this field (without this field of course) 	| The migration handler will set A string formatted the incoming version, the new one, and the date “1 → 2 \| date”                                                                                             	| x                                                                     	| x                                                                        	| x                                                                          	|
| **Some more explanations**|           | [xxx_]legacy is the only field you have to fill/serialize. Others are managed by migation handlers.                      	| The legacy field value is copied In the duplication one    	| It helps to understand what part of configuration is received by a Dedicated handler. 	| It helps to check the result Of the migration.                                                           	| It helps to check involved versions and when it happenned. /!\ Migration handlers of those  datastore/dataset/connectors Doesn’t check version and all actions  are executed wher migrate() method is called. 	| it helps to check to identify side effect between migration  parts.   	| it helps to check to identify side effect between migration  parts.      	| it helps to check to identify side effect between migration  parts.        	|

## Version 50

The version of datastore / dataset / connectors is defined in `AbstractConfig#VERSION`, and all has the same verison. A new attribute has been added in the datastore : `dso_shouldNotBeEmpty`. In datastore migration handler, if incoming version is inferior to 50 (arbitrary treshold version) `dso_shouldNotBeEmpty` is set with `Not empty` value.
Sink & source connectors will throw an exception if `dso_shouldNotBeEmpty` is not set.
The current version of the connector is 100.

## How to test migration handlers

From the root of this project, build the `migration-tester` module:

    mvn clean install -pl migration-tester -am
    
Then, go into the module and execute the web-tester:

    cd migration-tester
    mvn talend-component:web -Dtalend.web.port=8080
    
The component server is started and the migration tester plugin loaded. We can now call api of component server. Here is description https://talend.github.io/component-runtime/main/1.1.26/rest-openapi.html.

In following example, I use `jq` tool to filter return json payload : https://stedolan.github.io/jq/.

Here is how to retrieve ids :

    # Retrieve datastore id
    curl http://localhost:8080/api/v1/configurationtype/index | jq -r '.nodes[] | select(.configurationType == "datastore") | .id'
    bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZGF0YXN0b3JlI0RhdGFzdG9yZQ
    
    # Retrieve dataset id
    curl http://localhost:8080/api/v1/configurationtype/index | jq -r '.nodes[] | select(.configurationType == "dataset") | .id'
    bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZGF0YXNldCNEYXRhc2V0
    
    # Retrieve mapper / sink ids
    curl http://localhost:8080/api/v1/component/index | jq -r '.components[] | select(.categories[] == "Migration/Tester") | .id.id'
    bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZHVtbXlTb3VyY2U
    bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjRHVtbXlTaW5r
    
So to test migration task of datastore :

      curl -X POST "http://localhost:8080/api/v1/configurationtype/migrate/bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZGF0YXN0b3JlI0RhdGFzdG9yZQ/1" -H "accept: application/json" -H "Content-Type: application/json" -d '{"configuration.dso_legacy": "my_value"}' | jq .
         % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                        Dload  Upload   Total   Spent    Left  Speed
       100   569    0   529  100    40   172k  13333 --:--:-- --:--:-- --:--:--  185k
       {
         "configuration.dso_migration_handler_callback": "1 -> 100 | 2020/11/07 01:14:33",
         "configuration.dso_legacy": "my_value",
         "configuration.dso_duplication": "my_value",
         "configuration.dso_incoming": "{\n\t\"dso_legacy\" : \"my_value\"\n}",
         "configuration.dso_shouldNotBeEmpty": "Not empty",
         "configuration.dso_outgoing": "{\n\t\"dso_legacy\" : \"my_value\",\n\t\"dso_incoming\" : \"{\n\t\"dso_legacy\" : \"my_value\"\n}\",\n\t\"dso_duplication\" : \"my_value\",\n\t\"dso_migration_handler_callback\" : \"1 -> 100 | 2020/11/07 01:14:33\"\n}"
       }

We can se that we send a json payload with only the `dso_legacy` configuration key. The returned payload is the migrated datastore configuration payload.

Here are the same calls for other parts:

The dataset migration :

    curl -X POST "http://localhost:8080/api/v1/configurationtype/migrate/bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZGF0YXNldCNEYXRhc2V0/1" -H "accept: application/json" -H "Content-Type: application/json" -d '{"configuration.dse_legacy": "my_value"}' | jq .
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100   572    0   532  100    40  53200   4000 --:--:-- --:--:-- --:--:-- 63555
    {
      "configuration.dso.dso_from_dataset": "from dataset",
      "configuration.dse_legacy": "my_value",
      "configuration.dse_migration_handler_callback": "1 -> 100 | 2020/11/07 01:14:33",
      "configuration.dse_incoming": "{\n\t\"dse_legacy\" : \"my_value\"\n}",
      "configuration.dse_outgoing": "{\n\t\"dse_duplication\" : \"my_value\",\n\t\"dse_incoming\" : \"{\n\t\"dse_legacy\" : \"my_value\"\n}\",\n\t\"dse_legacy\" : \"my_value\",\n\t\"dse_migration_handler_callback\" : \"1 -> 100 | 2020/11/07 01:14:33\"\n}",
      "configuration.dse_duplication": "my_value"
    }

The source connector configuration migration. The endpoint is `component/migrate` :

    curl -X POST "http://localhost:8080/api/v1/component/migrate/bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjZHVtbXlTb3VyY2U/1" -H "accept: application/json" -H "Content-Type: application/json" -d '{"configuration.source_legacy": "my_value"}' | jq .
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100   744    0   701  100    43   171k  10750 --:--:-- --:--:-- --:--:--  181k
    {
      "configuration.dse.dso.dso_from_source": "from source",
      "configuration.source_legacy": "my_value",
      "configuration.source_duplication": "my_value",
      "configuration.source_outgoing": "{\n\t\"configuration.source_legacy\" : \"my_value\",\n\t\"configuration.source_migration_handler_callback\" : \"1 -> 100 | 2020/11/07 01:14:33\",\n\t\"configuration.source_incoming\" : \"{\n\t\"configuration.source_legacy\" : \"my_value\"\n}\",\n\t\"configuration.source_duplication\" : \"my_value\"\n}",
      "configuration.source_migration_handler_callback": "1 -> 100 | 2020/11/07 01:14:33",
      "configuration.source_incoming": "{\n\t\"configuration.source_legacy\" : \"my_value\"\n}",
      "configuration.dse.dse_from_source": "from source"
    }

The sink connector configuration migration :

    curl -X POST "http://localhost:8080/api/v1/component/migrate/bWlncmF0aW9uLXRlc3RlciNUZXN0ZXIjRHVtbXlTaW5r/1" -H "accept: application/json" -H "Content-Type: application/json" -d '{"configuration.sink_legacy": "my_value"}' | jq .
      % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                     Dload  Upload   Total   Spent    Left  Speed
    100   716    0   675  100    41  67500   4100 --:--:-- --:--:-- --:--:-- 71600
    {
      "configuration.sink_migration_handler_callback": "1 -> 100 | 2020/11/07 01:14:33",
      "configuration.dse.dso.dso_from_sink": "from source",
      "configuration.sink_duplication": "my_value",
      "configuration.sink_legacy": "my_value",
      "configuration.sink_incoming": "{\n\t\"configuration.sink_legacy\" : \"my_value\"\n}",
      "configuration.sink_outgoing": "{\n\t\"configuration.sink_legacy\" : \"my_value\",\n\t\"configuration.sink_migration_handler_callback\" : \"1 -> 100 | 2020/11/07 01:14:33\",\n\t\"configuration.sink_duplication\" : \"my_value\",\n\t\"configuration.sink_incoming\" : \"{\n\t\"configuration.sink_legacy\" : \"my_value\"\n}\"\n}",
      "configuration.dse.dse_from_sink": "from sink"
    }

## The source return one row

The source connector, return one row that is the recieved configuration. This configuration must be migrated.
