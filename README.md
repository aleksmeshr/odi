## OrientDB Database Interface for Erlang

Tested on OrientDB version 1.5.0.

### Connect without database

{ok, C} = odi:connect(Host, Username, Password, Opts).

Parameters:

 - Host      - server host;
 - Username  - server username, stored in config;
 - Password  - server password, stored in config;
 - Opts      - property list of extra options. Supported properties:
     - {port,     Integer}    server port, default: 2424
     - {timeout,  Integer}    socket connect timeout and network message timeout, default 5000 ms.

Example:

  {ok, C} = odi:connect("127.0.0.1", "server_user", "server_password", []).

### Connect to database

  odi:db_open(Host, Database, Username, Password, []).

Parameters:

 - Host      - server host;
 - Database  - database name;
 - Username  - database username;
 - Password  - database password;
 - Opts      - property list of extra options, see connect/4.

Returns:

  {{[{ClusterName, Id, Type}], DistributedConfig}, C}

 - ClusterName       - name of the cluster;
 - Id                - id of the cluster;
 - Type              - type of the cluster;
 - DistributedConfig - distributed servers cluster config, null if no.

Example:

  {{ClustersList, null}, C} = odi:db_open("127.0.0.1", "demo",  "admin", "admin", []).

### Disconnect

  odi:close(C).

### Syncronous SQL Query

  odi:query_sync(C, QueryText, Limit).

  Parameters:

 - QueryText - SQL query text;
 - Limit     - limit number of returning records.

  Returns:

    [{RID, RecordType, RecordVersion, RecordContent}]

 - RID = {ClusterId, ClusterPosition}
 - RecordType = document | flat | raw
 - RecordVersion = integer() - version of a record at this RID
 - RecordContent = {Class, [{FieldName, FieldValue}]} if RecordType = document
 - RecordContent = binary() if RecordType = flat | raw

  Example:

    [{{22,0},document,1,{<<"City">>,[{<<"name">>,<<"Rome">>},{<<"country">>,{link,{23,0}}}]}}] =
    odi:query_sync(C, "SELECT FROM City WHERE name=\"Rome\"", 1).

### Add record

  odi:record_create(C, ClusterId, RecordContent, RecordType, Mode).

  Parameters:

 - Mode = sync | async

  Returns:

 - ClusterPosition - positon of created record in the cluster

  Example:

    Position = odi:record_create(C, 23, {<<"Country">>, [{<<"name">>, <<"Japan">>}]}, document, sync).

### Update record

  odi:record_update(C, RID, RecordContent, RecordVersion, RecordType, Mode).

  Returns:

 - NewRecordVersion

  Example:

    odi:record_update(C,{23,171},{<<"Country">>, [{<<"name">>, <<"Japan">>}]},6, document,sync).

### Load record

  odi:record_load(C, RID, FetchPlan).

  Parameters:

 - FetchPlan  - fetch plan controls whitch linked records will be returned, default meens "*:1".

  Returns:

    {RecordType , RecordVersion, RecordContent, LinkedRecords}

    LinkedRecords = [{RID, RecordType, RecordVersion, RecordContent}]

  Example:

    odi:record_load(C,{22,1},default).

### Delete record

  odi:record_delete(C, RID, RecordVersion, Mode).

  Returns:

 - true  - record deleted
 - false - record not deleted, RecordVersion not matched

  Example:

    odi:record_delete(C,{23,Pos1},1,sync).

### Other functions:

 + db_exist(C, DatabaseName).
 + odi:db_create(C, DatabaseName, DatabaseType, StorageType).
 + odi:db_size(C).
 + odi:db_reload(C).
 + odi:db_delete(C, DatabaseName, ServerStorageType).
 + odi:db_countrecords(C).
 + odi:datacluster_add(C, Type, Name, Location, DataSegmentName).
 + odi:datacluster_remove(C, ClusterId).
 + odi:datacluster_count(C, [ClusterId]).
 + odi:datacluster_datarange(C, ClusterId).
 + odi:datasegment_add(C, Name, Location).
 + odi:datasegment_remove(C, Name).
 + odi:query_async/4 will be implemented later
 + odi:tx_commit/4 will be implemented later

### Error handling

  If any error occurs, function returns {error, Description}.