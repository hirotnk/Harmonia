-module(hm_ds).
-behaviour(gen_server).
-vsn('0.1').

-export([
        fun_for_data/2,
        fun_for_index/2,
        get/1,
        get/3,
        name/1,
        start_link/1,
        stop/1,
        store/2,
        store/3
        ]).
-export([
        gather_get/4,
        lookup_data_table_solo/6
        ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").

-define(MODULE_NAME, atom_to_list(?MODULE)).
-define(hm_global_table, hm_table_global).


start_link(RegName) ->
    gen_server:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop() ->
    gen_server:cast({global, ?MODULE}, stop).

stop(RegName) ->
    gen_server:cast({global, name(RegName)}, stop).

terminate(_Reason, _State) ->
    ok.

get(Key) ->
    TargetName = hm_router:lookup(Key),
    SuccListTarget = gen_server:call({global, TargetName}, copy_succlist),
    SuccList = hm_misc:make_request_list(TargetName, SuccListTarget),
    get_from_succlist(SuccList, Key).


%% @spec(store(Key::atom(), Value::any() ) -> {ok, Cnt::integer()} | 
%%                                            {partial, Cnt::integer()} |
%%                                            {ng, Msg::string}
store(Key, Value) ->
    store_in(?hm_global_table, Key, Value).

gather_get(0,   Pid, Ref, ResList) -> 
    ?info_p("gather_get OK:ResList:[~p]~n", gather_get, [ResList]),
    Pid ! {ok, Ref, ResList};
gather_get(Cnt, Pid, Ref, ResList) ->
    receive
        {ok, Ref, RowList} ->
            gather_get( Cnt - 1, Pid, Ref, [RowList|ResList] );
        {error, Ref, Msg} ->
            ?error_p("gather_get Error:Msg:[~p]~n", gather_get, [Msg]),
            gather_get( Cnt - 1, Pid, Ref, ResList )
    after ?TIMEOUT_GET ->
        Pid ! {error, Ref, timeout}
    end.

get(DomainName, TableName, Cond) ->
    DTName = list_to_atom(DomainName ++ TableName),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    case lookup_index_table(NodeList, DTName, Cond) of
        {ok, DataNodeList, AttList, [MS]} ->

            {_, Flist} = lists:foldl(fun ?MODULE:fun_for_data/2, {1,[]}, AttList),

            NodeCnt = length(DataNodeList), Ref = make_ref(),

            % spawn a gathering process
            LoopPid = 
                spawn_link(
                    ?MODULE, 
                    gather_get, 
                    [NodeCnt, self(), Ref, []]
                ),

            % spawn a process for each node
            lists:foreach(
                   fun(El) -> 
                        spawn_link(
                            ?MODULE, 
                            lookup_data_table_solo, 
                            [El, DTName, Flist, MS, LoopPid, Ref]
                        )
                   end, 
                   DataNodeList
            ),
            receive
                {ok, Ref, Result} -> {ok, Result};
                {error, Ref, Msg} -> {error, Msg} 
            after ?TIMEOUT_GET ->
                ?error_p("get timeout :DomainName:[~p] TabName:[~p] Cond:[~p]~n",
                        get, [DomainName, TableName, Cond])
            end;
        {error, Msg} -> {error, Msg}
    end.

lookup_data_table_solo(NodeName, DTNameTable, FlistModified, MS, LoopPid, Ref) ->
    DataNodeName = name(list_to_atom(atom_to_list(NodeName) -- ?PROCESS_PREFIX)),

    case hm_misc:is_alive(DataNodeName) of
        false -> 
            ?warning_p("DataNode Not Alive : Node:[~p].~n", none, [DataNodeName]),
            LoopPid ! {error, Ref, node_not_alive};
        true ->
            {ok, RowList} = 
                gen_server:call(
                    {global, DataNodeName}, 
                    {select_table, ?hm_global_table, DTNameTable, FlistModified, MS}
                ),
            LoopPid ! {ok, Ref, RowList}
    end.

make_select_cond(AttList, Cond, Fun) ->
    {_, FlistModified} = lists:foldl(Fun, {1,[]}, AttList),
    MS = hm_qp:parse(hm_qp:scan(Cond, AttList)),
    {ok, FlistModified, MS}.

lookup_index_table(NodeList, DTNameTable, Cond) ->
    case hm_misc:get_first_alive_entry(NodeList) of 
        {error, none} -> {error, no_node_available};
        {IndexNode, _Vector} ->
            IndexNodeDs = name(list_to_atom(atom_to_list(IndexNode) -- ?PROCESS_PREFIX)),
            IndexNodeTable = hm_table:name(list_to_atom(atom_to_list(IndexNode) -- ?PROCESS_PREFIX)),
            case gen_server:call({global, IndexNodeTable}, {get_table_info, DTNameTable}) of
                {ok, Tid, AttList} ->
                    Fun = fun ?MODULE:fun_for_index/2,
                    {ok, FlistModified, MS} = make_select_cond(AttList, Cond, Fun),
                    {ok, RowList} = 
                        gen_server:call({global, IndexNodeDs}, {select_table, Tid, FlistModified, MS}),
                    UniqNodeList = 
                        sets:to_list(
                            sets:from_list(
                                lists:foldl(fun (Row, AccIn) -> AccIn ++ [hd(Row)] end, [], RowList))),
                    {ok, UniqNodeList, AttList, MS};
                {error, Msg} -> 
                    {error, Msg}
            end
    end.

fun_for_index({_Fname, Bool, _},{N,FList}) ->
    case Bool =:= true of
        true -> {N+1, FList ++ [list_to_atom("$" ++ integer_to_list(N+1))]};
        false -> {N, FList}
    end.

fun_for_data(_T,{N,Flist}) ->
    {N+1, Flist ++ [list_to_atom("$" ++ integer_to_list(N+1))]}.

lookup_data_table([], _DTNameTable, _FlistModified, _MS, RecList) ->
    {ok, RecList};
lookup_data_table(UniqNodeList, DTNameTable, FlistModified, MS, RecList) ->
    NodeName = hd(UniqNodeList),
    DataNodeName = name(list_to_atom(atom_to_list(NodeName) -- ?PROCESS_PREFIX)),

    case hm_misc:is_alive(DataNodeName) of
        false -> 
            ?warning_p("DataNode Not Alive : Node:[~p].~n", none, [DataNodeName]),
            lookup_data_table(tl(UniqNodeList), DTNameTable, FlistModified, MS, RecList);
        true ->
            {ok, RowList} = 
                gen_server:call(
                    {global, DataNodeName}, 
                    {select_table, ?hm_global_table, DTNameTable, FlistModified, MS}
                ),
            NewRecList = RecList ++ RowList,
            lookup_data_table(tl(UniqNodeList), DTNameTable, FlistModified, MS, NewRecList)
    end.


store(DomainName, TableName, KVList) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    {IndexTableNode, _} = hd(NodeList), % index record is stored on this node in DTName table
    case hm_table:get_table_info(DomainName, TableName, NodeList) of
        {ok, _Tid, AttList} ->
            DTName = list_to_atom(DomainName++TableName),
            {ok, Key} = calc_key_from_key_data(DTName, KVList, AttList),
            DataTableNode = hm_router:lookup(Key), % data record is stored on this node in ?hm_global_table table
            case store_data(DTName, DataTableNode, KVList) of
                {ok, _}      -> 
                    store_index(DTName, DataTableNode, IndexTableNode, KVList, AttList);
                {error, Msg} -> {error, Msg}
            end;
        {error, ErrInfo} -> {error, ErrInfo}
    end.


%%
%% @spec store_sta(DTName::atom(), DataTableNode::atom(), KVList::list()) ->
%%                                           {ok, Cnt::integer()} | 
%%                                           {partial, Cnt::integer()}
%%                                           {ng, Msg::string} |
%%                 
%% @doc
%% store data of data filed in the following record form:
%% Row: {DmainName++TableName::atom(), [{Fld1, Value}, {Fld2, Value}, ...]}
%% Key : calculate from key fields' data
%% Target Nodes: calculate from Key
%%
store_data(DTName, DataTableNode, KVList) ->
    VList = lists:map(fun({_Fld,Val}) -> Val end, KVList),
    store_in_to(DataTableNode, ?hm_global_table, {DTName, VList}).
%%
%% @spec store_index(DTName::atom(), DataTableNode::atom(), 
%%                   IndexTableNode::atom(), KVList::list(), AttList::list()) ->
%%                                           {ok, Cnt::integer()} | 
%%                                           {partial, Cnt::integer()}
%%                                           {ng, Msg::string} |
%
%% @doc 
%% store index data to global_table in the following record form:
%% RouterName indicates the successor from the key DTName.
%% Data: {RouterName, [{Fld1, Value}, {Fld2, Value}, ...]}
%% Table: DomainName ++ TableName
store_index(DTNameTable, DataTableNode, IndexTableNode, KVList, AttList) ->
    {ok, Row} = extract_kv_tuples(KVList, AttList, true),
    Vlist = 
        lists:foldl(
            fun
                ({_Fname,Value}, AccIn) ->
                    AccIn ++ [Value]
            end, [], Row),
    store_in_to(IndexTableNode, DTNameTable, {DataTableNode, Vlist}).

%% retrieve tuples of fields with data
%% KVFlag: true -> returns key-tuples list
%% KVFlag: false -> returns data-tuples list
extract_kv_tuples(KVList, AttList, KVFlag) ->
    DataFields = 
        lists:filter( 
            fun({_Fname, BoolVal, _Init}) -> 
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
%% @spec(calc_key_from_key_data(DomainName, TableName, KVList, AttList) -> {ok, Key::atom()}
%% @doc generates key from DTName and key values
%% @TODO: the type of data in key fields are now confined to following:
%%        may need to figure out more flexibility
%%        - string()
%%        - atom()
%%        - integer()
calc_key_from_key_data(DTName, KVList, AttList) ->
    % retrieve tuples of key fields
    {ok, KeyList} = 
        extract_kv_tuples(KVList, AttList, true),

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
    RouterName = hm_router:lookup(Key),
    store_in_to(RouterName, TableName, {Key, Value}).


store_in_to(RouterName, TableName, {Key, Value}) ->
    % store to all successor list nodes
    SuccListTemp = gen_server:call({global, RouterName}, copy_succlist),
    SuccList = hm_misc:make_request_list(RouterName, SuccListTemp),
    store_to_succlist(SuccList, TableName, Key, Value, {length(SuccList), 0}).

% the successor list here includes target node itself, and
% tail of successor list
store_to_succlist([], _TableName, _Key, _Value, {Len, Cnt}) -> 
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
    case global:whereis_name(TargetName) of 
        undefined -> 
            % TODO: when fail to store data, or node is down, write logging to  
            %       another file and continue processing
            ?error_p("store_to_succlist:Target undefined ~nKey:[~p] TargetName:[~p].~n", 
                store, [Key, TargetName]),
            NewCnt = Cnt;
        _Pid      -> 
            ?info_p("store_to_succlist:Key:[~p] TargetName:[~p].~n", store, [Key, TargetName]),
            Reply = gen_server:call({global, TargetName}, {store, TableName, Key, Value}),
            case Reply of
                {ok, _Msg} -> NewCnt = Cnt + 1;
                {error, {store, Msg}} -> 
                    ?error_p("store_to_succlist:~p undefined ~nKey:[~p] TargetName:[~p].~n", 
                        store, [Msg, Key, TargetName]),
                    NewCnt = Cnt
            end
    end,
    store_to_succlist(tl(SuccList), TableName, Key, Value, {Len, NewCnt}).

%% TODO: clean this interfaces of get_from_succlist
get_from_succlist([], _Key) -> {error, nodata};
get_from_succlist(Succlist, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(list_to_atom( atom_to_list(RouterName) -- ?PROCESS_PREFIX )),
    case global:whereis_name(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), Key);
        _Any ->
            Result = gen_server:call({global, TargetName}, {get, Key}),
            ?info_p("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> get_from_succlist(tl(Succlist), Key);
                _Any -> Result
            end
    end.

init(RegName) ->
    GlobalTableId = ets:new(?hm_global_table, [duplicate_bag, public]),
    {ok, {RegName, [{?hm_global_table, GlobalTableId}]}}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({register_table, {TableName, TableId}}, _From, {RegName, TableList}) ->
    ?info_p("register_table info:[~p] new table:[~p].~n", RegName, [TableList, {TableName, TableId}]),
    {reply, {ok, register_table}, {RegName, [{TableName, TableId}|TableList]}};

handle_call({get_table_info, DTName}, _From, {_RegName, TableList}=State) ->
    ReplyData = hm_misc:search_table_attlist(DTName, TableList),
    {reply, ReplyData, State};

handle_call({select_table, ?hm_global_table, DTName, FlistModified, MS}, _From, {_RegName, TableList}=State) ->
    case lists:keyfind(?hm_global_table, 1, TableList) of
        false -> {reply, {nb, no_global_table}, State};
        {_,Tid} ->
            % ets:select(16400, [{{'$1',['$2','$3','$4']},[{'and',{'==','$2',yyy},{'==', '$1', 'Domain1Tbl2'}}],['$$']}]).
            Reply = ets:select(Tid, [{{'$1',FlistModified},[{'and',{'==','$1',DTName},MS}],['$$']}]),
            {reply, {ok, Reply}, State}
    end;

handle_call({select_table, Tid, FlistModified, MS}, _From, State) ->

    Reply = ets:select(Tid, [{{'$1', FlistModified},MS,['$$']}]),
    {reply, {ok, Reply}, State};


handle_call({store, TableName, Key, Value}, _From, {RegName, TableList}) ->
    case lists:keyfind(TableName, 1, TableList) of 
        false -> 
            {reply, {error, {store, no_table_found}}, {RegName, TableList}};
        {TableName, TableId} ->
            _Ret = ets:insert(TableId, {Key, Value}), % ets:insert always return true
            {reply, {ok, insert}, {RegName, TableList}}
    end;

handle_call({get, TableId, Key}, _From, {RegName, GlobalTableId}) ->
    Reply = ets:lookup(TableId, Key),
    {reply, Reply, {RegName, GlobalTableId}};

handle_call({get, TableName, _Key, {Op,Val}=_Cond}, _From, {RegName, GlobalTableId}) ->
    % list of {key, nodename}
    Reply = ets:select(TableName,[{ {'$1','$2'}, [ { Op, '$1', Val } ],['$_'] }]),
    {reply, Reply, {RegName, GlobalTableId}}.

name(Name) -> list_to_atom(?MODULE_NAME ++ "_" ++ atom_to_list(Name)).
