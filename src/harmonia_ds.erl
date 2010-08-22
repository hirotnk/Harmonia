-module(harmonia_ds).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/1, stop/1, store/2, store/3, get/1, name/1, 
         create_bag/1, get/3, put/3]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).


-include("harmonia.hrl").
-define(hm_global_table, hm_table_global).


start_link(RegName) ->
    gen_server:start_link({local, name(RegName)}, ?MODULE, RegName, []).

stop() ->
    gen_server:cast(?MODULE, stop).

stop(RegName) ->
    gen_server:cast(name(RegName), stop).

terminate(_Reason, _State) -> ok.

create_bag(BagName) ->
    store(?hm_global_table, BagName).

put(BagName, Key, Value) ->
    NodeName = harmonia:lookup(Key),
    store(BagName, BagName, {Key, NodeName}),
    store(BagName, Key, {Key, Value}).

%% get(BagName, Key, Cond) ->
%%     TargetName = harmonia:lookup(BagName),
%%     SuccList = gen_server:call(TargetName, copy_succlist),
%% 
%%     % return list of { key, nodename }
%%     NodeList = get_from_succlist(SuccList, BagName, Key, Cond), 
%% 
%%     % ask every node in parallel
%%     issue_retrieval(BagName, NodeList, []).


issue_retrieval(BagName, [], Acc) -> Acc;
issue_retrieval(BagName, NodeList, Acc) ->
    {Key, NodeName} = hd(NodeList),

    SuccList = gen_server:call(NodeName, copy_succlist),
    Result = get_from_succlist(SuccList, BagName, Key),
    issue_retrieval(BagName, tl(NodeList), [Result|Acc]).



get(Key) ->
    TargetName = harmonia:lookup(Key),
    SuccListTarget = gen_server:call(TargetName, copy_succlist),
    SuccList = hm_misc:make_request_list(TargetName, SuccListTarget),
    get_from_succlist(SuccList, Key).


%% spec(store(Key::atom(), Value::any() ) -> {ok, Cnt::integer()} | 
%%                                           {partial, Cnt::integer()}
%%                                           {ng, Msg::string} |
store(Key, Value) ->
    store_in(?hm_global_table, Key, Value).


get(DomainName, TableName, Cond) ->
    DTName = list_to_atom(DomainName ++ TableName),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    case lookup_index_table(NodeList, DTName, Cond) of
        Any -> Any;
        % {ok, DataNodeList} -> 
            % case lookup_data_table(DataNodeList, DTName, Cond) of
            %     {ok, RecordList} -> {ok, RecordList};
            %     {error, Msg} -> {error, Msg} 
            % end;
        {error, Msg} -> {error, Msg}
    end.

lookup_index_table(NodeList, DTNameTable, Cond) ->
    case hm_misc:get_first_alive_entry(NodeList) of 
        {error, none} -> {error, no_node_available};
        {IndexNode, _Vector} ->
            IndexNodeDs = name(list_to_atom(atom_to_list(IndexNode) -- ?PROCESS_PREFIX)),
            IndexNodeTable = harmonia_table:name(list_to_atom(atom_to_list(IndexNode) -- ?PROCESS_PREFIX)),
            case gen_server:call(IndexNodeTable, {get_table_info, DTNameTable}) of
                
                {ok, Tid, AttList} ->
                    gen_server:call(IndexNodeDs, {select_table, DTNameTable, Tid, AttList, Cond});
                {error, Msg} -> 
                    {error, Msg}
            end
    end.

store(DomainName, TableName, KVList) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    {IndexTableNode, _} = hd(NodeList), % index record is stored on this node in DTName table
    case harmonia_table:get_table_info(DomainName, TableName, NodeList) of
        {ok, Tid, AttList} ->
            DTName = list_to_atom(DomainName++TableName),
            {ok, Key} = calc_key_from_key_data(DTName, KVList, AttList),
            DataTableNode = harmonia:lookup(Key), % data record is stored on this node in ?hm_global_table table
            case store_data(DTName, DataTableNode, KVList) of
                {ok, _}      -> 
                    store_index(DTName,
                                DataTableNode,
                                IndexTableNode,
                                KVList, AttList);
                {error, Msg} -> {error, Msg}
            end;
        {error, ErrInfo} -> {error, ErrInfo}
    end.


%% store data of data filed in the following record form:
%% Table: ?hm_global_table
%% Row: {DmainName++TableName::atom(), [{Fld1, Value}, {Fld2, Value}, ...]}
%% Key : calculate from key fields' data
%% Target Nodes: calculate from Key
store_data(DTName, DataTableNode, KVList) ->
    store_in_to(DataTableNode, ?hm_global_table, {DTName, KVList}).

%% store index data to global_table in the following record form:
%% RouterName indicates the successor from the key DTName.
%% Data: {RouterName, [{Fld1, Value}, {Fld2, Value}, ...]}
%% Table: DomainName ++ TableName
store_index(DTNameTable, DataTableNode, IndexTableNode, KVList, AttList) ->
    {ok, Row} = extract_kv_tuples(DTNameTable, KVList, AttList, true),
    Vlist = 
        lists:foldl(
            fun
                ({Fname,Value}, AccIn) ->
                    AccIn ++ [Value]
            end, [], Row),
    store_in_to(IndexTableNode, DTNameTable, {DataTableNode, Vlist}).

%% retrieve tuples of fields with data
%% KVFlag: true -> returns key-tuples list
%% KVFlag: false -> returns data-tuples list
extract_kv_tuples(DTName, KVList, AttList, KVFlag) ->
    DataFields = 
        lists:filter( 
            fun({Fname, BoolVal, Init}) -> 
                case BoolVal =:= KVFlag of 
                    true -> true; 
                    false -> false 
                end 
            end, 
            AttList),
    StoreDataList = 
        lists:filter(
            fun({Fname,_}) -> 
                case lists:keyfind(Fname, 1, DataFields) of 
                    false -> false;
                    _ -> true
                end
            end, 
            KVList),
     {ok, StoreDataList}.

%%
%% spec(calc_key_from_key_data(DomainName, TableName, KVList, AttList) -> {ok, Key::atom()}
%% description: generates key from key values
calc_key_from_key_data(DTName, KVList, AttList) ->
    % retrieve tuples of key fields
    {ok, KeyList} = 
        extract_kv_tuples(DTName, KVList, AttList, true),

    % the value of the key fields must be list/string()
    Key = lists:foldl(fun
                        ({_, Data},AccIn) -> 
                            case is_list(Data) of
                                true -> AccIn ++ Data ;
                                false -> 
                                    case is_atom(Data) of
                                        true ->
                                            AccIn ++ atom_to_list(Data);
                                        false ->
                                            AccIn ++ integer_to_list(Data)
                                    end
                            end
                      end, 
                      [], KeyList),
    {ok, list_to_atom(atom_to_list(DTName)++Key)}.

store_in(TableName, Key, Value) ->
    RouterName = harmonia:lookup(Key),
    store_in_to(RouterName, TableName, {Key, Value}).


store_in_to(RouterName, TableName, {Key, Value}) ->
    % store to all successor list nodes
    SuccListTemp = gen_server:call(RouterName, copy_succlist),
    SuccList = hm_misc:make_request_list(RouterName, SuccListTemp),
    ?debug_p("store:SuccList:[~p] RouterName:[~p].~n", store, [SuccList, RouterName]),
    store_to_succlist(SuccList, TableName, Key, Value, {length(SuccList), 0}).

% the successor list here includes target node itself, and
% tail of successor list
store_to_succlist([], TableName, Key, Value, {Len, Cnt}) -> 
    ?debug_p("store_to_succlist:LINE:[~p] Tab:[~p] Key:[~p] Value:[~p] count:[~p].~n", 
        store, [?LINE, TableName, Key, Value, {Len, Cnt}]),
    case Len =:= Cnt of
        true -> {ok, Cnt};
        false -> 
            case Cnt =:= 0 of
                true -> {ng, Cnt};
                false -> {partial, Cnt}
            end
    end;
store_to_succlist(SuccList, TableName, Key, Value, {Len, Cnt}) ->
    {RouterName, _} = hd(SuccList),

    TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(RouterName)))),
    case whereis(TargetName) of 
        undefined -> 
            % TODO: when fail to store data, or node is down, write logging to  
            %       another file and continue processing
            ?debug_p("***ERROR*** store_to_succlist:Target undefined ~nKey:[~p] TargetName:[~p].~n", 
                store, [Key, TargetName]),
            NewCnt = Cnt;
        _Pid      -> 
            ?debug_p("store_to_succlist:Key:[~p] TargetName:[~p].~n", store, [Key, TargetName]),
            Reply = gen_server:call(TargetName, {store, TableName, Key, Value}),
            case Reply of
                true -> NewCnt = Cnt + 1;
                false -> NewCnt = Cnt
            end
    end,
    store_to_succlist(tl(SuccList), TableName, Key, Value, {Len, NewCnt}).

%% TODO: clean this interfaces of get_from_succlist
get_from_succlist([], _Key) -> {error, nodata};
get_from_succlist(Succlist, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), Key);
        _ ->
            Result = gen_server:call(TargetName, {get, Key}),
            ?debug_p("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> 
                    get_from_succlist(tl(Succlist), Key);
                _ -> 
                    Result
            end
    end.



get_from_succlist([], BagName, _Key) -> {error, nodata};
get_from_succlist(Succlist, BagName, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), BagName, Key);
        _ ->
            Result = gen_server:call(TargetName, {get, BagName, Key}),
            ?debug_p("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> 
                    get_from_succlist(tl(Succlist), BagName, Key);
                _ -> 
                    Result
            end
    end.
get_from_succlist([],      _BagName, _Key, _Cond) -> [{error, nodata}];
get_from_succlist(Succlist, BagName, Key, Cond) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), BagName, Key, Cond);
        _ ->
            % get list of [{key, nodename},..]
            Result = gen_server:call(TargetName, {get, BagName, Key, Cond}),

            ?debug_p("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> 
                    get_from_succlist(tl(Succlist), BagName, Key, Cond);
                _ -> 
                    Result
            end
    end.

init(RegName) ->
    GlobalTableId = ets:new(?hm_global_table, [ordered_set, public]),
    {ok, {RegName, [{?hm_global_table, GlobalTableId}]}}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({register_table, {TableName, TableId}}, _From, {RegName, TableList}) ->
    ?debug_p("register_table info:[~p] new table:[~p].~n", RegName, [TableList, {TableName, TableId}]),
    {reply, {ok, register_table}, {RegName, [{TableName, TableId}|TableList]}};

handle_call({get_table_info, DTName}, _From, {RegName, TableList}=State) ->
    ReplyData = hm_misc:search_table_attlist(DTName, TableList),
    {reply, ReplyData, State};

handle_call({select_table, DTName, Tid, AttList, Cond}, _From, {RegName, TableList}=State) ->

    ?debug_p("select_table:TableList:[~p].~n", RegName, [TableList]),
    {_, FlistModified} = 
            lists:foldl(fun
                ({Fname, Bool, _},{N,FList}) ->
                    case Bool =:= true of
                        true -> {N+1, FList ++ [list_to_atom("$" ++ integer_to_list(N+1))]};
                        false -> {N, FList}
                    end
            end, {1,[]}, AttList),
    ?debug_p("select_table:FlistModified:[~p].~n", RegName, [FlistModified]),
    MS = hm_qp:parse(hm_qp:scan(Cond, AttList)),
    ?debug_p("select_table:MS:[~p].~n", RegName, [MS]),
    ?debug_p("select_table:All:[~p].~n", RegName, [[{{'$1', FlistModified}, MS, ['$$']}]]),
    Reply = ets:select(Tid, [{{'$1', FlistModified},MS,['$$']}]),
    {reply, Reply, State};


handle_call({store, TableName, Key, Value}, _From, {RegName, TableList}) ->
    case lists:keyfind(TableName, 1, TableList) of 
        false -> 
            {reply, {error, {store, no_table_found}}, {RegName, TableList}};
        {TableName, TableId} ->
            Ret = ets:insert(TableId, {Key, Value}),
            {reply, Ret, {RegName, TableList}}
    end;

handle_call({get, TableId, Key}, _From, {RegName, GlobalTableId}) ->
    Reply = ets:lookup(TableId, Key),
    {reply, Reply, {RegName, GlobalTableId}};

handle_call({get, TableName, Key, {Op,Val}=Cond}, _From, {RegName, GlobalTableId}) ->
    % list of {key, nodename}
    Reply = ets:select(TableName,[{ {'$1','$2'}, [ { Op, '$1', Val } ],['$_'] }]),
    {reply, Reply, {RegName, GlobalTableId}}.

name(Name) -> list_to_atom("harmonia_ds_" ++ atom_to_list(Name)).

