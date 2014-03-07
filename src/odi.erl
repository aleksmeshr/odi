%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.

-module(odi).

-export([start_link/0,
         connect/4, connect/5,
         close/1,
         db_open/5,
         db_open/6,
         db_create/4,
         db_close/1,
         db_exist/2,
         db_reload/1,
         db_delete/3,
         db_size/1,
         db_countrecords/1,
         datacluster_add/5,
         datacluster_remove/2,
         datacluster_count/2,
         datacluster_datarange/2,
         datasegment_add/3,
         datasegment_remove/2,
         record_create/5,
         record_load/3,
         record_update/6,
         record_delete/4,
         query_async/4,
         query_sync/3,
         command/2,
         script/2,
         tx_commit/4]).

-include("../include/odi.hrl").

-define(DEFAULT_TIMEOUT, infinity).

%% -- client interface --

start_link() ->
    odi_sock:start_link().

%This is the first operation requested by the client when it needs to work with the server instance without openning a database.
%Returns the session_id:integer of the client.
%Opts (proplists) = port (default 2424), timeout (default 5000 ms).
connect(Host, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

connect(C, Host, Username, Password, Opts) ->
    call_conn(C, {connect, Host, Username, Password, Opts}).

%This is the first operation the client should call. It opens a database on the remote OrientDB Server.
%Returns the Session-Id to being reused for all the next calls and the list of configured clusters.
db_open(Host, DBName, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    db_open(C, Host, DBName, Username, Password, Opts).
db_open(C, Host, DBName, Username, Password, Opts) ->
    call_conn(C, {db_open, Host, DBName, Username, Password, Opts}).

%Creates a database in the remote OrientDB server instance.
%Works in connect-mode.
db_create(C, DatabaseName, DatabaseType, StorageType) ->
    call(C, {db_create, DatabaseName, DatabaseType, StorageType}).

%Closes the database and the network connection to the OrientDB Server instance.
%No return is expected. The socket is also closed.
db_close(C) ->
    call(C, {db_close}).

%Asks if a database exists in the OrientDB Server instance. It returns true (non-zero) or false (zero).
%Works in connect-mode.
db_exist(C, DatabaseName) ->
    to_bool(call(C, {db_exist, DatabaseName})).

%Reloads database information.
db_reload(C) ->
    call(C, {db_reload}).

%Removes a database from the OrientDB Server instance.
%Works in connect-mode.
db_delete(C, DatabaseName, ServerStorageType) ->
    call(C, {db_delete, DatabaseName, ServerStorageType}).

%Returns size of the opened database.
db_size(C) ->
    call(C, {db_size}).

%Asks for the number of records in a database in the OrientDB Server instance.
db_countrecords(C) ->
    call(C, {db_countrecords}).

%Add a new data cluster.
datacluster_add(C, Type, Name, Location, DataSegmentName) ->
    call(C, {datacluster_add, Type, Name, Location, DataSegmentName}).

%Remove a cluster.
datacluster_remove(C, ClusterId) ->
    call(C, {datacluster_remove, ClusterId}).

%Returns summary count of records in one or more clusters.
%   ClustersIds = [cluster_id1, ...]
datacluster_count(C, ClustersIds) ->
    call(C, {datacluster_count, ClustersIds}).

%Returns the range of record ids for a cluster.
datacluster_datarange(C, ClusterId) ->
    call(C, {datacluster_datarange, ClusterId}).

%Add a new data segment.
datasegment_add(C, Name, Location) ->
    call(C, {datasegment_add, Name, Location}).

%Drop a data segment.
datasegment_remove(C, Name) ->
    call(C, {datasegment_remove, Name}).

%Create a new record. Returns the position in the cluster of the new record.
%New records can have version > 0 (since v1.0) in case the RID has been recycled.
%   Response: (ClusterPosition:long)
record_create(C, ClusterId, {Class, Fields}, RecordType, Mode) ->
    record_create(C, ClusterId, odi_doc:encode(Class, Fields), RecordType, Mode);
record_create(C, ClusterId, RecordContent, RecordType, Mode) ->
    call(C, {record_create, ClusterId, RecordContent, RecordType, Mode}).

%Load a record by RecordID, according to a fetch plan
%   Response: [(payload-status:byte)[(record-content:bytes)(record-version:int)(record-type:byte)]*]+
record_load(C, {ClusterId, ClusterPosition}, FetchPlan) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    call(C, {record_load, ClusterId, ClusterPosition, FetchPlan2}).

%Update a record. Returns the new record's version.
%   RecordVersion: current record version
%   RecordType: raw, flat, document
%   Mode: sync, async
%Returns NewRecordVersion:integer
record_update(C, RID, {Class, Fields}, RecordVersion, RecordType, Mode) ->
    record_update(C, RID, odi_doc:encode(Class, Fields), RecordVersion, RecordType, Mode);
record_update(C, {ClusterId, ClusterPosition}, RecordContent, RecordVersion, RecordType, Mode) ->
    call(C, {record_update, ClusterId, ClusterPosition, RecordContent, RecordVersion, RecordType, Mode}).

%Delete a record by its RecordID. During the optimistic transaction the record will be deleted only if the versions match.
%Returns true if has been deleted otherwise false.
record_delete(C, {ClusterId, ClusterPosition}, RecordVersion, Mode) ->
    to_bool(call(C, {record_delete, ClusterId, ClusterPosition, RecordVersion, Mode})).

%Syncronous SQL query (SELECT or TRAVERSE).
query_sync(C, SQL, Limit) ->
    call(C, {command_sync, SQL, Limit, select}).

%Asyncronous SQL query, not fully implemented
query_async(C, SQL, Limit, FetchPlan) ->
    call(C, {command_async, SQL, Limit, FetchPlan}).

%Syncronous SQL command.
command(C, SQL) ->
    call(C, {command_sync, SQL, -1, command}).

%Syncronous SQL command.
script(C, JavaScript) ->
    call(C, {command_sync, JavaScript, -1, script}).

%Commits a transaction. This operation flushes all the pending changes to the server side.
%   Operations: [{OperationType, ClusterId, ClusterPosition, RecordType}]
tx_commit(C, TxId, UsingTxLog, Operations) ->
    call(C, {tx_commit, TxId, UsingTxLog, Operations}).

close(C) ->
    odi_sock:close(C).

%% -- internal functions --

call_conn(C, Command) ->
    {call(C, Command), C}.

call(C, Command) ->
    case gen_server:call(C, Command, infinity) of
        Error = {error, _} -> Error;
        {R} -> R;
        R -> R
    end.

to_bool(Num) ->
    case Num of
        _ when Num > 0 -> true;
        _ when Num == 0 -> false;
        Error -> Error
    end.