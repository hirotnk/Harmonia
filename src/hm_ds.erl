% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(hm_ds).
-behaviour(gen_server).
-vsn('0.1').

-export([
        cget/1,
        cstore/2,
        fun_for_data/2,
        fun_for_index/2,
        get/1,
        rget/3,
        name/1,
        start_link/1,
        stop/0,
        stop/1,
        store/2,
        rstore/3
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

terminate(Reason, State) ->
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    ok.

%% @spec(cget(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%%
%% @doc Key::atom()|string()|integer()
get(Key) ->
    get_in(Key).

%% @spec(cget(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%%
%% @doc Key::atom()|string()|integer()
%%      simple K/V store with cache enabled
cget(Key) ->
    cget_in(Key).

%% @spec(store(Key::atom(), Value::any() ) -> 
%%              {ok, Cnt::integer()} | {partial, Cnt::integer()} | {ng, Msg::string}
%%
%% @doc simple K/V store
store(Key, Value) ->
    store_in(Key, Value).

%% @spec(cstore(Key::atom(), Value::any() ) -> 
%%              {ok, Cnt::integer()} | {partial, Cnt::integer()} | {ng, Msg::string}
%%
%% @doc simple K/V store with cache enabled
cstore(Key, Value) ->
    cstore_in(Key, Value).

%% @spec(rget(DomainName::string(), TableName::string(), Cond::string())) ->
%%              {ok, Rec::list()} | {error, Msg::any()}
%%
%% @doc Range Query API
%%      Returned list does not include DTName
rget(DomainName, TableName, Cond) ->
    get_in(DomainName, TableName, Cond).

%% @spec(rstore(DomainName::string(), TableName::string(), KVList)) ->
%%              {ok, Rec::list()} | {error, Msg::any()}
%%
%% @doc Range Query API
%%      KVList: [{FieldName::string(), Value::any()}]
%%
rstore(DomainName, TableName, KVList) ->
    store_in(DomainName, TableName, KVList).


%% ----------------------------------------------------------------------------
%% Internal Functions
%% ----------------------------------------------------------------------------

get_in(Key) ->
    TargetName = hm_router:lookup(Key),
    SuccListTarget = gen_server:call({global, TargetName}, copy_succlist),
    SuccList = hm_misc:make_request_list(TargetName, SuccListTarget),
    get_from_succlist(SuccList, Key).

cget_in(Key) ->
    case hm_cache:get_cache(Key) of
        none ->
            case hm_ds:get(Key) of
                {ok, {Key, Value}} -> 
                    hm_cache:store_cache(Key, Value),
                    {ok, {Key, Value}};
                {error, Msg} -> 
                    {error, Msg}
            end;

        {ok, {Value, _Cnt}} ->
            {ok, {Key, Value}}
    end.

cstore_in(Key, Value) ->
    hm_cache:store_cache(Key, Value),
    store_in(Key, Value).

get_in(DomainName, TableName, Cond) ->
    DTName = list_to_atom(DomainName ++ TableName),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    {ok, IndexTableNode, IndexDsNode} =  lookup_index_table_node(NodeList),
    {ok, _Tid, AttList} =  lookup_index_table_attribute(IndexTableNode, DTName),
    {ok, MS, MSData, FlistIndex, FlistData} = get_query_spec(Cond, AttList),
    {ok, DataNodeList} = get_data_node_list(IndexDsNode, DTName, FlistIndex, MS),
    scatter_gather(DomainName, DataNodeList, DTName, FlistData, MSData, TableName, Cond).


scatter_gather(DomainName, DataNodeList, DTName, FlistData, MSData, TableName, Cond) ->
    Ref = make_ref(),

    % spawn a gathering process
    LoopPid = spawn_link( ?MODULE, gather_get, [length(DataNodeList), self(), Ref, []]),

    % spawn a scattering processes for each node
    lists:foreach(
           fun(El) -> 
                spawn_link(
                    ?MODULE, 
                    lookup_data_table_solo, 
                    [El, DTName, FlistData, MSData, LoopPid, Ref]
                )
           end, 
           DataNodeList
    ),
    % receive total result
    receive
        {ok, Ref, Result} -> {ok, Result};
        {error, Ref, Msg} -> {error, Msg} 
    after ?TIMEOUT_GET ->
        ?error_p("get timeout :DomainName:[~p] TabName:[~p] Cond:[~p]~n",
                get, [DomainName, TableName, Cond]),
        {error, timeout}
    end.

get_query_spec(Cond, AttList) ->
    MS     = hm_qp:parse(hm_qp:scan(Cond, {index, AttList})),
    [MSData] = hm_qp:parse(hm_qp:scan(Cond, {data, AttList})),
    {_, FlistIndex} = lists:foldl(fun ?MODULE:fun_for_index/2, {1,[]}, AttList),
    {_, FlistData}  = lists:foldl(fun ?MODULE:fun_for_data/2, {1,[]}, AttList),
    {ok, MS, MSData, FlistIndex, FlistData}.


get_data_node_list(IndexDsNode, DTName, FlistIndex, MS) ->
    {ok, RowList} = gen_server:call({global, IndexDsNode}, {select_table, DTName, FlistIndex, MS}),
    DataNodeList = 
        sets:to_list(
            sets:from_list(
                lists:foldl(fun (Node, AccIn) -> [hd(Node)] ++ AccIn  end, [], RowList))),
    {ok, DataNodeList}.


lookup_index_table_attribute(IndexTableNode, DTNameTable) ->
    case gen_server:call({global, IndexTableNode}, {get_table_info, DTNameTable}) of
        {ok, Tid, AttList} -> {ok, Tid, AttList};
        {error, Msg} -> 
            {error, Msg}
    end.

store_in(DomainName, TableName, KVList) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    {IndexTableNode, _} = hd(NodeList), % index record is stored on this node in DTName table
    case hm_table:get_table_info(DomainName, TableName, NodeList) of
        {ok, _Tid, AttList} ->

            %% Key is calcurated with DTName+values of key fields
            DTName = list_to_atom(DomainName++TableName),
            {ok, Key} = calc_key_from_key_data(DTName, KVList, AttList),

            %% data record is stored on this node in ?hm_global_table table
            %% at first data is stored, then index is stored
            DataTableNode = hm_router:lookup(Key), 
            case store_data(DTName, DataTableNode, KVList) of
                {ok, _}      -> 
                    store_index(DTName, DataTableNode, IndexTableNode, KVList, AttList);
                {error, Msg} -> 
                    ?error_p("store_data failed :DTName:[~p] DataTableNode:[~p] KVList:[~p] Msg:[~p].~n", 
                        none, [DTName, DataTableNode, KVList, Msg]),
                    {error, Msg}
            end;
        {error, ErrInfo} -> {error, ErrInfo}
    end.

gather_get(0,   Pid, Ref, ResList) -> 
    ?info_p("gather_get OK:ResList:[~p]~n", gather_get, [ResList]),
    Pid ! {ok, Ref, lists:usort(ResList)};
gather_get(Cnt, Pid, Ref, ResList) ->
    receive
        {ok, Ref, RowList} ->
            gather_get( Cnt - 1, Pid, Ref, lists:merge(RowList,ResList) );
        {error, Ref, Msg} ->
            ?error_p("gather_get Error:Msg:[~p]~n", gather_get, [Msg]),
            gather_get( Cnt - 1, Pid, Ref, ResList )
    after ?TIMEOUT_GET ->
        Pid ! {error, Ref, timeout}
    end.

lookup_data_table_solo(NodeName, DTNameTable, FlistModified, MS, LoopPid, Ref) ->
    DataNodeName = name(atom_to_list(NodeName) -- ?PROCESS_PREFIX),

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
                LoopPid ! {ok, Ref, lists:map(fun({_,Rec}) -> Rec end, RowList)}
    end.


lookup_index_table_node(NodeList) ->
    case hm_misc:get_first_alive_entry(NodeList) of 
        {error, none} -> {error, no_node_available};
        {IndexNode, _Vector} ->
            IndexTableNode = hm_table:name(atom_to_list(IndexNode) -- ?PROCESS_PREFIX),
            IndexNodeDs = name(atom_to_list(IndexNode) -- ?PROCESS_PREFIX),
            {ok, IndexTableNode, IndexNodeDs}
    end.

fun_for_index({_Fname, Bool, _},{N,FList}) ->
    case Bool =:= true of
        true -> {N+1, FList ++ [list_to_atom("$" ++ integer_to_list(N+1))]};
        false -> {N, FList}
    end.

fun_for_data(_T,{N,Flist}) ->
    {N+1, Flist ++ [list_to_atom("$" ++ integer_to_list(N+1))]}.

%%
%% @spec store_sta(DTName::atom(), DataTableNode::atom(), KVList::list()) ->
%%                                           {ok, Cnt::integer()} | 
%%                                           {partial, Cnt::integer()}
%%                                           {ng, Msg::string} |
%%                 
%% @doc
%% store data of all fileds in the following record form:
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
            fun({_Fname,Value}, AccIn) ->
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

    % the value of the key fields must be list/string()/atom
    Key = lists:foldl(fun
                        ({_, Data},AccIn) when is_list(Data) -> 
                                AccIn ++ Data;

                        ({_, Data},AccIn) when is_atom(Data) -> 
                                AccIn ++ atom_to_list(Data);

                        ({_, Data},AccIn) when is_integer(Data) -> 
                                AccIn ++ integer_to_list(Data)
                      end, 
                      [], KeyList),
    {ok, list_to_atom(atom_to_list(DTName)++Key)}.

store_in(Key, Value) ->
    RouterName = hm_router:lookup(Key),
    store_in_to(RouterName, ?hm_global_table, {Key, Value}).


store_in_to(RouterName, TableName, {Key, Value}) ->
    % store to all successor list nodes
    SuccListTemp = gen_server:call({global, RouterName}, copy_succlist),
    SuccList = hm_misc:make_request_list(RouterName, SuccListTemp),
    store_to_succlist(SuccList, TableName, Key, Value, {length(SuccList), 0}).

% the successor list here includes target node itself, and
% tail of successor list
store_to_succlist([], _TableName, _Key, _Value, {Len, Cnt}) -> 
    case Cnt of
        Len  -> {ok, Cnt};
        0    -> {ng, Cnt};
        _Else -> {partial, Cnt}
    end;
store_to_succlist(SuccList, TableName, Key, Value, {Len, Cnt}) ->
    {RouterName, _} = hd(SuccList),

    TargetName = name(atom_to_list(RouterName) -- ?PROCESS_PREFIX),
    case global:whereis_name(TargetName) of
        undefined ->
            ?error_p("store_to_succlist:Target undefined ~nKey:[~p] TargetName:[~p].~n",
                store, [Key, TargetName]),
            NewCnt = Cnt;
        _      ->
            ?info_p("store_to_succlist:Key:[~p] TargetName:[~p].~n", store, [Key, TargetName]),
            Reply = gen_server:call({global, TargetName}, {store, TableName, Key, Value}),
            case Reply of
                {ok, _} -> NewCnt = Cnt + 1;
                {error, Msg} ->
                    ?error_p("store_to_succlist:~p undefined ~nKey:[~p] TargetName:[~p].~n",
                        store, [Msg, Key, TargetName]),
                    NewCnt = Cnt
            end
    end,
    store_to_succlist(tl(SuccList), TableName, Key, Value, {Len, NewCnt}).

get_from_succlist([], _Key) -> {error, nodata};
get_from_succlist(Succlist, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(atom_to_list(RouterName) -- ?PROCESS_PREFIX),
    case global:whereis_name(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), Key);
        _ ->
            Result = gen_server:call({global, TargetName}, {get, Key}),
            ?info_p("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> get_from_succlist(tl(Succlist), Key);
                _ -> {ok, Result}
            end
    end.

init(RegName) ->
    GlobalTableId = ets:new(?hm_global_table, [bag, public, named_table]),
    {ok, {RegName, [{?hm_global_table, GlobalTableId}]}}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({register_table, {TableName, TableId}}, _From, {RegName, TableList}) ->
    ?info_p("register_table info:[~p] new table:[~p].~n", RegName, [TableList, {TableName, TableId}]),
    {reply, {ok, register_table, TableName}, {RegName, [{TableName, TableId}|TableList]}};

handle_call({unregister_table, TableName}, _From, {RegName, TableList}) ->
    ?info_p("unregister_table info:[~p] new table:[~p].~n", RegName, [TableList, TableName]),
    NewTableList = lists:keydelete(TableName, 1, TableList),
    {reply, {ok, unregister_table}, {RegName, NewTableList}};

handle_call({get_table_info, DTName}, _From, {_RegName, TableList}=State) ->
    ReplyData = hm_misc:search_table_attlist(DTName, TableList),
    {reply, ReplyData, State};

handle_call({select_table, ?hm_global_table, DTName, FlistModified, MS}, _From, {_RegName, _TableList}=State) ->
    % ets:select(Tid, [{{'$1',['$2','$3','$4']},[{'and',{'==','$2',yyy},{'==', '$1', 'Domain1Tbl2'}}],['$$']}]).
    Reply = ets:select(?hm_global_table, [{{'$1',FlistModified},[{'and',{'==','$1',DTName},MS}], ['$_']}]),
    {reply, {ok, lists:usort(Reply)}, State};

handle_call({select_table, DTName, FlistModified, MS}, _From, State) ->
    Reply = ets:select(DTName, [{{'$1', FlistModified},MS,['$$']}]),
    {reply, {ok, lists:usort(Reply)}, State};

handle_call({store, TableName, Key, Value}, _From, {RegName, TableList}) ->
    case lists:keyfind(TableName, 1, TableList) of 
        false -> 
            {reply, {error, {store, no_table_found}}, {RegName, TableList}};
        {TableName, TableId} ->
            Ret = (catch ets:insert(TableId, {Key, Value})),
            case Ret of
                true ->
                    {reply, {ok, insert}, {RegName, TableList}};
                Any ->
                    {reply, 
                     {error, {store, exception, TableName, {Key, Value}, Any}}, 
                     {RegName, TableList}
                    }
            end
    end;

handle_call({get, Key}, _From, {RegName, TableList}) ->
    Reply = ets:lookup(?hm_global_table, Key),
    {reply, Reply, {RegName, TableList}};

handle_call({get, TableId, Key}, _From, {RegName, TableList}) ->
    Reply = ets:lookup(TableId, Key),
    {reply, Reply, {RegName, TableList}}.

name(Name) when is_list(Name) -> list_to_atom(?MODULE_NAME ++ "_" ++ Name);
name(Name) -> list_to_atom(?MODULE_NAME ++ "_" ++ atom_to_list(Name)).

