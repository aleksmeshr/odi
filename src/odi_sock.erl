%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.

-module(odi_sock).

-behavior(gen_server).

-export([start_link/0,
         close/1,
         get_parameter/2]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-export([on_response/3, command/2]).

-include("../include/odi.hrl").

-record(state, {mod,    %socket module: gen_tcp or ssl(unsupported)
                sock,   %opened socket
				protocolVersion = 0,
                session_id = -1, %OrientDB session Id
                open_mode, %connection opened with: connect() | db_open()
                data = <<>>, %received data from socket
                queue = queue:new(), %commands queue
                timeout = 5000 %network timeout
                }).

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

% --- support functions ---

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).

%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

terminate(_Reason, State) ->
    handle_cast(stop, State),
    ok.

handle_call(Command, From, #state{queue=Q, timeout=Timeout} = State) ->
    Req = {{call, From}, Command},
    case command(Command, State#state{queue = queue:in(Req, Q)}) of
        {noreply, State2} -> {noreply, State2, Timeout};
        Error -> Error
    end.

handle_cast({{Method, From, Ref}, Command} = Req, State)
        when (Method == cast), is_pid(From), is_reference(Ref) ->
    #state{queue = Q} = State,
    command(Command, State#state{queue = queue:in(Req, Q)});

handle_cast(stop, #state{sock = Sock} = State) ->
    case is_port(Sock) of
        true -> gen_tcp:close(Sock);
        _ -> false
    end,
    {stop, normal, flush_queue(State, {stop, closed})}.

flush_queue(#state{queue = Q} = State, Error) ->
    case queue:is_empty(Q) of
        false ->
            flush_queue(finish(State, Error), Error);
        true -> State
    end.

handle_info(timeout, State) ->
    {stop, timeout, flush_queue(State, {error, timeout})};

% Receive messages from socket:
% on socket close
handle_info({Closed, Sock}, #state{sock = Sock} = State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {stop, sock_closed, flush_queue(State, {error, sock_closed})};

% on socket error
handle_info({Error, Sock, Reason}, #state{sock = Sock} = State)
  when Error == tcp_error; Error == ssl_error ->
    Why = {sock_error, Reason},
    {stop, Why, flush_queue(State, {error, Why})};

% socket ok
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State};

% socket is not ok
handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, flush_queue(State, {error, Status})};

% receive data from socket
handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}).

%Handle code change
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% -- Commands processing --

% ?
% command(Command, State = #state{sync_required = true})
%   when Command /= sync ->
%     {noreply, finish(State, {error, sync_required})};

%This is the first operation requested by the client when it needs to work with the server instance without openning a database. 
%It returns the session id of the client.
%  Request:  (driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)(user-name:string)(user-password:string)
%  Response: (session-id:int)
command({connect, Host, Username, Password, Opts}, State) ->
    % % storing login data in the process dictionary for security reasons?
    % put(username, Username),
    % put(password, Password),
    State2 = pre_connect(State, Host, Opts),
    sendRequest(State2, ?O_CONNECT,
        [string, string, short, string, string, string],
        [?O_DRV_NAME, ?O_DRV_VER, ?O_PROTO_VER, null, Username, Password]),
    inet:setopts(State2#state.sock, [{active, true}]),
    {noreply, State2};

%This is the first operation the client should call. It opens a database on the remote OrientDB Server.
%Returns the Session-Id to being reused for all the next calls and the list of configured clusters.
%  Request:  (driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)
%           (database-name:string)(database-type:string)(user-name:string)(user-password:string)
%  Response: (session-id:int)(num-of-clusters:short)
%           [(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)]
            %(cluster-config:bytes)(orientdb-release:string)
%dbType = document | graph.
command({db_open, Host, DBName, Username, Password, Opts}, State) ->
    % % storing login data in the process dictionary for security reasons?
    % put(username, Username),
    % put(password, Password),
    case pre_connect(State, Host, Opts) of
        {error, _} = E -> {noreply, finish(State, E)};
        {ok, State2} ->
            erlang:display([?O_DRV_NAME, ?O_DRV_VER, ?O_PROTO_VER, null, DBName, Username, Password]),
            sendRequest(State2, ?O_DB_OPEN,
                [string, string, short, string, string, string, string],
                [?O_DRV_NAME, ?O_DRV_VER, ?O_PROTO_VER, null, DBName, Username, Password]),
            inet:setopts(State2#state.sock, [{active, true}]),
            {noreply, State2}
    end;

%Creates a database in the remote OrientDB server instance
%   Request:  (database-name:string)(database-type:string)(storage-type:string)
%   Response: empty
command({db_create, DatabaseName, DatabaseType, StorageType}, State) ->
    sendRequest(State, ?O_DB_CREATE,
        [string, string, string],
        [DatabaseName, DatabaseType, StorageType]),
    {noreply, State};

%Closes the database and the network connection to the OrientDB Server instance. No return is expected. The socket is also closed.
%  Request:  empty
%  Response: no response, the socket is just closed at server side
command({db_close}, State) ->
    sendRequest(State, ?O_DB_CLOSE, [], []),
    handle_cast(stop, State),
    {stop, normal, State};

%Asks if a database exists in the OrientDB Server instance. It returns true (non-zero) or false (zero).
%   Request:  (database-name:string) <-- before 1.0rc1 this was empty (server-storage-type:string - since 1.5-snapshot)
%   Response: (result:byte)
command({db_exist, DatabaseName}, State) ->
    sendRequest(State, ?O_DB_EXIST,
        [string],
        [DatabaseName]),
    {noreply, State};

%Reloads database information. Available since 1.0rc4.
%   Request:  empty
%   Response:(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)]
command({db_reload}, State) ->
    sendRequest(State, ?O_DB_RELOAD, [], []),
    {noreply, State};

%Removes a database from the OrientDB Server instance.
%It returns nothing if the database has been deleted or throws a OStorageException if the database doesn't exists.
%   Request:  (database-name:string)(server-storage-type:string - since 1.5-snapshot)
%   Response: empty
command({db_delete, DatabaseName, ServerStorageType}, State) ->
    sendRequest(State, ?O_DB_DELETE,
        [string, string],
        [DatabaseName, ServerStorageType]),
    {noreply, State};

%Asks for the size of a database in the OrientDB Server instance.
%   Request:  empty
%   Response: (size:long)
command({db_size}, State) ->
    sendRequest(State, ?O_DB_SIZE, [], []),
    {noreply, State};

%Asks for the number of records in a database in the OrientDB Server instance.
%   Request:  empty
%   Response: (count:long)
command({db_countrecords}, State) ->
    sendRequest(State, ?O_DB_COUNTRECORDS, [], []),
    {noreply, State};

%Add a new data cluster.
%   Request:  (type:string)(name:string)(location:string)(datasegment-name:string)
%   Response: (new-cluster:short)
command({datacluster_add, Type, Name, Location, DataSegmentName}, State) ->
    sendRequest(State, ?O_DATACLUSTER_ADD,
        [string, string, string, string],
        [Type, Name, Location, DataSegmentName]),
    {noreply, State};

%Remove a cluster.
%   Request:  (cluster-number:short)
%   Response: (delete-on-clientside:byte)
command({datacluster_remove, ClusterId}, State) ->
    sendRequest(State, ?O_DATACLUSTER_REMOVE,
        [short],
        [ClusterId]),
    {noreply, State};

%Returns the number of records in one or more clusters.
%   Request:  (cluster-count:short)[cluster-number:short](count-tombstones:byte)
%   Response: (records-in-clusters:long)
command({datacluster_count, ClustersIds}, State) ->
    sendRequest(State, ?O_DATACLUSTER_COUNT,
        {short, [short]},
        [[N] || N <- ClustersIds]),
    {noreply, State};

%Returns the range of record ids for a cluster.
%   Request:  (cluster-number:short)
%   Response: (begin:long)(end:long)
command({datacluster_datarange, ClusterId}, State) ->
    sendRequest(State, ?O_DATACLUSTER_DATARANGE,
        [short],
        [ClusterId]),
    {noreply, State};

%Add a new data segment.
%   Request:  (name:string)(location:string)
%   Response: (new-datasegment-id:int)
command({datasergment_add, Name, Location}, State) ->
    sendRequest(State, ?O_DATASEGMENT_ADD,
        [string, string],
        [Name, Location]),
    {noreply, State};

%Drop a data segment.
%   Request:  (name:string)
%   Response: (succeeded:boolean)
command({datasergment_remove, Name}, State) ->
    sendRequest(State, ?O_DATASEGMENT_REMOVE,
        [string],
        [Name]),
    {noreply, State};

%Create a new record. Returns the position in the cluster of the new record. New records can have version > 0 (since v1.0) in case the RID has been recycled.
%   Request:  (datasegment-id:int)(cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)
%   Response: (cluster-position:long)(record-version:int)
command({record_create, ClusterId, RecordContent, RecordType, Mode}, State) ->
    sendRequest(State, ?O_RECORD_CREATE,
        [short, bytes, byte, byte],
        [ClusterId, RecordContent,
        odi_bin:encode_record_type(RecordType),
        odi_bin:switch(Mode == sync, 0, 1)]),
    {noreply, State};

%Load a record by RecordID, according to a fetch plan
%   Request:  (cluster-id:short)(cluster-position:long)(fetch-plan:string)(ignore-cache:byte)(load-tombstones:byte)
%   Response: [(payload-status:byte)[(record-content:bytes)(record-version:int)(record-type:byte)]*]+
command({record_load, ClusterId, ClusterPosition, FetchPlan}, State) ->
    sendRequest(State, ?O_RECORD_LOAD,
        [short, long, string],
        [ClusterId, ClusterPosition, FetchPlan]),
    {noreply, State};

%Update a record. Returns the new record's version.
%   Request:  (cluster-id:short)(cluster-position:long)(record-content:bytes)(record-version:int)(record-type:byte)(mode:byte)
%   Response: (record-version:int)
command({record_update, ClusterId, ClusterPosition, RecordContent, RecordVersion, RecordType, Mode}, State) ->
    sendRequest(State, ?O_RECORD_UPDATE,
        [short, long, bytes, integer, byte, byte],
        [ClusterId, ClusterPosition, RecordContent,
        RecordVersion, %odi_bin:encode_record_vc(RecordVersionControl),
        odi_bin:encode_record_type(RecordType),
        odi_bin:switch(Mode == sync, 0, 1)]),
    {noreply, State};

%Delete a record by its RecordID. During the optimistic transaction the record will be deleted only if the versions match.
%Returns true if has been deleted otherwise false.
%   Request:  (cluster-id:short)(cluster-position:long)(record-version:int)(mode:byte)
%   Response: (payload-status:byte)
command({record_delete, ClusterId, ClusterPosition, RecordVersion, Mode}, State) ->
    sendRequest(State, ?O_RECORD_DELETE,
        [short, long, integer, byte],
        [ClusterId, ClusterPosition, RecordVersion,
        odi_bin:switch(Mode == sync, 0, 1)]),
    {noreply, State};

%Executes remote commands.
%   Request:  (mode:byte)(class-name:string)(command-payload-length:int)(command-payload)
%   Response:
%   - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
%   - asynchronous commands: [(asynch-result-type:byte)[(asynch-result-content:?)]*](pre-fetched-record-size)[(pre-fetched-record)]*+
command({command_async, QueryText, Limit, FetchPlan}, State) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    CommandPayload = odi_bin:encode([string, string, integer, string, rawbytes],
        ["com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery",
        QueryText, Limit, FetchPlan2, <<0:?o_int, 0:?o_int, 0:?o_int>>]),
    sendRequest(State, ?O_COMMAND,
        [byte, bytes],
        [$a, CommandPayload]),
    {noreply, State};

command({command_sync, QueryText, Limit, QueryType}, State) ->
    ClassName = case QueryType of
        select -> "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery";
        command -> "com.orientechnologies.orient.core.sql.OCommandSQL";
        script -> "com.orientechnologies.orient.core.command.script.OCommandScript"
    end,
    %TODO: different SQL Script command payload
    CommandPayload = odi_bin:encode([string, string, integer, rawbytes],
        [ClassName,
        QueryText, Limit, <<0:?o_int, 0:?o_int, 0:?o_int>>]),
    sendRequest(State, ?O_COMMAND,
        [byte, bytes],
        [$s, CommandPayload]),
    {noreply, State};

%generic_query:$s,CommandPayload("com.orientechnologies.orient.core.sql.OCommandSQL",QueryText)

%Commits a transaction. This operation flushes all the pending changes to the server side.
%   Request:  (tx-id:int)(using-tx-log:byte)[(operation-type:byte)(cluster-id:short)(cluster-position:long)(record-type:byte)<record-content>]*(0-byte indicating end-of-records)
%   Response: (created-record-count:int)[(client-specified-cluster-id:short)(client-specified-cluster-position:long)(created-cluster-id:short)
%       (created-cluster-position:long)]*(updated-record-count:int)[(updated-cluster-id:short)(updated-cluster-position:long)(new-record-version:int)]*
%   Operations: [[OperationType, ClusterId, ClusterPosition, RecordType]]
command({tx_commit, TxId, UsingTxLog, Operations}, State) ->
    sendRequest(State, ?O_TX_COMMIT,
        [integer, byte, {zero_end, [byte, short, long, byte]}],
        [TxId, UsingTxLog, Operations]),
    {noreply, State};

command(_Command, State) ->
    {error, State}.

% support functions ---

pre_connect(State, Host, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, 5000),
    Port = proplists:get_value(port, Opts, 2424),
    SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}],
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} -> {ok, State#state{mod = gen_tcp, sock = Sock, timeout = Timeout}};
        {error, _} = E -> E
    end.

sendRequest(#state{mod = Mod, sock = Sock, session_id = SessionId}, CommandType, Types, Values) ->
    Data = <<CommandType:?o_byte, SessionId:?o_int, (iolist_to_binary(odi_bin:encode(Types, Values)))/binary>>,
    %erlang:display({send, binary_to_list(Data)}),
    do_send(Mod, Sock, Data).

% port_command() more efficient then gen_tcp:send()
do_send(gen_tcp, Sock, Bin) ->
    try erlang:port_command(Sock, Bin) of
        true ->
            ok
    catch
        error:_Error ->
            {error,einval}
    end;

do_send(ssl, _Sock, _Bin) ->
    {error, ssl_unsupported}.

finish(State, Result) ->
    finish(State, Result, Result).

finish(State = #state{queue = Q}, _Notice, Result) ->
    case queue:get(Q) of
        % {{cast, From, Ref}, _} ->
        %     From ! {self(), Ref, Result};
        % {{incremental, From, Ref}, _} ->
        %     From ! {self(), Ref, Notice};
        {{call, From}, _} ->
            gen_server:reply(From, Result)
    end,
    State#state{queue = queue:drop(Q)}.

command_tag(#state{queue = Q}) ->
    case queue:len(Q) == 0 of
        true -> none;
        false ->
            {_, Req} = queue:get(Q),
            if is_tuple(Req) ->
                    element(1, Req);
               is_atom(Req) ->
                    Req
            end
    end.

%% -- backend message handling --

%main loop
loop(#state{data = Data, timeout = Timeout} = State) -> %timeout = Timeout
    Cmd = command_tag(State),
    %erlang:display({recv, Cmd, binary_to_list(Data)}),
    %erlang:display({recv, Cmd, size(Data)}),
    case Cmd of
        none -> {noreply, #state{data = <<>>}};
        _ ->
            case byte_size(Data) > 0 of
                true ->
                    case on_response(Cmd, Data, State) of
                        {fetch_more, State2} -> {noreply, State2, Timeout};
                        {noreply, #state{data = <<>>} = State2} -> {noreply, State2};
                        {noreply, State2}                       -> loop(State2);
                        R = {stop, _Reason2, _State2}           -> R
                    end;
                false ->
                    {noreply, State}
            end
    end.

%Procced empty response message
on_empty_response(Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            State2 = finish(State#state{data = Rest}, {error, ErrorInfo});
        0 -> State2 = finish(State#state{data = Message}, ok)
    end,
    {noreply, State2}.

%Procced response message without changing State (excl. Data)
on_simple_response(Bin, State, Format) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            State2 = finish(State#state{data = Rest}, {error, ErrorInfo});
        0 -> {Result, Rest} = odi_bin:decode(Format, Message),
            State2 = finish(State#state{data = Rest}, Result)
    end,
    {noreply, State2}.

on_response(connect, Bin, #state{protocolVersion = ProtocolVersion, sock = Sock} = State) ->
	case ProtocolVersion of
		_ when ProtocolVersion == 0 ->
			<<ProtocolVersion2:?o_short, Rest/binary>> = Bin,
			State3 = State#state{protocolVersion = ProtocolVersion2, data = Rest};
		_ ->
			<<Status:?o_byte, _:?o_int, Message/binary>> = Bin,
            case Status of
                1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
                    State2 = State#state{data = Rest},
                    gen_tcp:close(Sock),
                    State3 = finish(State2, {error, ErrorInfo});
                0 -> <<SessionId:?o_int, Rest/binary>> = Message,
                    State2 = State#state{session_id = SessionId, open_mode = connect, data = Rest},
                    State3 = finish(State2, ok);
                _ ->
                    State2 = State#state{data = <<>>},
                    gen_tcp:close(Sock),
                    State3 = finish(State2, {error, error_server_response})
            end
		end,
	{noreply, State3};

% Response: (session-id:int)(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)](cluster-config:bytes)(orientdb-release:string)
on_response(db_open, Bin, #state{protocolVersion = ProtocolVersion, sock = Sock} = State) ->
	case ProtocolVersion of
		_ when ProtocolVersion == 0 ->
			<<ProtocolVersion2:?o_short, Rest/binary>> = Bin,
			{noreply, State#state{protocolVersion = ProtocolVersion2, data = Rest}};
		_ ->
			<<Status:?o_byte, _:?o_int, Message/binary>> = Bin,
            try
                case Status of
                    1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
                        gen_tcp:close(Sock),
                        {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
                    0 ->
                        {{SessionId, {_NumOfClusters, ClusterParams}, ClusterConfig}, Rest}
                            = odi_bin:decode([integer, {integer, [string, short, string]}, string], Message),
                        {noreply, finish(State#state{session_id = SessionId, open_mode = db_open, data = Rest},
                            {ClusterParams, ClusterConfig})};
                     _ ->
                        gen_tcp:close(Sock),
                        {noreply, finish(State#state{data = <<>>}, {error, error_server_response, Bin})}
                end
            catch
                _:_ -> {fetch_more, State}
            end
	end;

% Response: empty
on_response(db_create, Bin, State) ->
    on_empty_response(Bin, State);

% Response: none, socket closed.
on_response(db_close, _Bin, State) ->
    {stop, normal, finish(State, db_closed)};

% Response: empty
on_response(db_exist, Bin, State) ->
    on_simple_response(Bin, State, [byte]);

% Response:(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)]
on_response(db_reload, Bin, State) ->
    on_simple_response(Bin, State, [{short, [string, short, string, short]}, string, string]);

on_response(db_delete, Bin, State) ->
    on_empty_response(Bin, State);

% Response: (size:long)
on_response(db_size, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (count:long)
on_response(db_countrecords, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (new-cluster:short)
on_response(datacluster_add, Bin, State) ->
    on_simple_response(Bin, State, [short]);

% Response: (delete-on-clientside:byte)
on_response(datacluster_remove, Bin, State) ->
    on_simple_response(Bin, State, [byte]);

% Response: (records-in-clusters:long)
on_response(datacluster_count, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (begin:long)(end:long)
on_response(datacluster_datarange, Bin, State) ->
    on_simple_response(Bin, State, [long, long]);

% Response: (new-datasegment-id:int)
on_response(datasegment_add, Bin, State) ->
    on_simple_response(Bin, State, [int]);

% Response: (succeeded:boolean)
on_response(datasegment_remove, Bin, State) ->
    on_simple_response(Bin, State, [byte]);

% Response: [(payload-status:byte)[(record-content:bytes)(record-version:int)(record-type:byte)]*]+
on_response(record_load, Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    try case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
        0 -> <<PayloadStatus:?o_byte, Msg/binary>> = Message,
            case PayloadStatus of
                0 ->
                    {noreply, finish(State#state{data = Msg}, null)};
                _ ->
                    {Result, Rest} = odi_bin:decode_record(Msg),
                    {noreply, finish(State#state{data = Rest}, Result)}
            end
        end
    catch
        _:_ -> {fetch_more, State}
    end;

% Response: (cluster-position:long)(record-version:int)
on_response(record_create, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (record-version:int)
on_response(record_update, Bin, State) ->
    on_simple_response(Bin, State, [integer]);

% Response: (payload-status:byte)
on_response(record_delete, Bin, State) ->
    on_simple_response(Bin, State, [byte]);

% Response:
% - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
on_response(command_sync, Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    try case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
        0 -> <<ResultType:?o_byte, Msg/binary>> = Message,
            case ResultType of
                $l ->   %list of records
                    <<RecordsCount:?o_int, RecordsData/binary>> = Msg,
                    {Records, Rest} = odi_bin:decode_record_list(RecordsData, RecordsCount, []),
                    {noreply, finish(State#state{data = Rest}, Records)};
                $n ->   %null result
                    {noreply, finish(State#state{data = Msg}, null)};
                $r ->   %single record returned
                    {Record, Rest} = odi_bin:decode_linked_record(Msg),
                    {noreply, finish(State#state{data = Rest}, Record)};
                $c ->   %collection of records
                    %TODO: decode collection of records
                    {noreply, finish(State#state{data = Msg}, null)};
                $a ->   %serialized result
                    %TODO: serialized result
                    {noreply, finish(State#state{data = Msg}, null)};
                _ ->
                    {noreply, finish(State#state{data = <<>>}, command_sync_decode_error)}
            end
        end
    catch
        _:_ -> {fetch_more, State}
    end;

% Response:
% - asynchronous commands: [(asynch-result-type:byte)[(asynch-result-content:?)]*](pre-fetched-record-size)[(pre-fetched-record)]*+
on_response(command_async, Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    try case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
        0 -> <<ResultType:?o_byte, RecordData/binary>> = Message,
            case ResultType of
                $1 ->   %TODO: iterate reading ResultType and RecordData
                    {Record, Rest} = odi_bin:decode_linked_record(RecordData),
                    {noreply, finish(State#state{data = Rest}, Record)};
                $2 ->   %TODO: iterate reading ResultType and RecordData
                    {Record, Rest} = odi_bin:decode_linked_record(RecordData),
                    {noreply, finish(State#state{data = Rest}, Record)};
                ResultType ->
                    {noreply, finish(State#state{data = <<>>}, {null, ResultType})}
            end
        end
    catch
        _:_ -> {fetch_more, State}
    end;

% Response: (created-record-count:int)[(client-specified-cluster-id:short)(client-specified-cluster-position:long)
%        (created-cluster-id:short)(created-cluster-position:long)]*
%        (updated-record-count:int)[(updated-cluster-id:short)(updated-cluster-position:long)(new-record-version:int)]*
on_response(tx_commit, Bin, State) ->
    on_simple_response(Bin, State, [integer, short, long]);

on_response(_Command, _Bin, State) ->
    {error, State}.

% Error result: {stop, normal, finish(State, {error, Why})};

