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
%%%-------------------------------------------------------------------
%%% @author Yoshihiro TANAKA <hirotnkg@gmail.com>
%%% @copyright (C) 2010, hiro
%%% @doc data storage I/F
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro TANAKA <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_ds).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_server).
-vsn('0.1').

%% API
-export([
        cdel/1,
        cget/1,
        cstore/2,
        del/1,
        fun_for_data/2,
        fun_for_index/2,
        get/1,
        name/1,
        rdel/3,
        rget/3,
        rstore/3,
        start_link/1,
        stop/0,
        stop/1,
        store/2
        ]).
-export([
        gather_get/4,
        gather_store/4,
        gather_delete/4,
        delete_data_table_solo/6,
        lookup_data_table_solo/6,
        store_solo/6
        ]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("harmonia.hrl").

-define(MODULE_NAME, atom_to_list(?MODULE)).
-define(hm_global_table, hm_table_global).


%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(Env) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(RegName) ->
    gen_server:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop() ->
    gen_server:cast({global, ?MODULE}, stop).

stop(RegName) ->
    gen_server:cast({global, name(RegName)}, stop).

%%--------------------------------------------------------------------
%% @doc Key::atom()|string()|integer()
%% @spec(get(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%% @end
%%--------------------------------------------------------------------
get(Key) ->
    get_in(Key).

%%--------------------------------------------------------------------
%% @doc Key::atom()|string()|integer()
%%      simple K/V store with cache enabled
%% @spec(cget(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%% @end
%%--------------------------------------------------------------------
cget(Key) ->
    cget_in(Key).

%%--------------------------------------------------------------------
%% @doc Range Query API
%%      Returned list does not include DTName
%% @spec(rget(DomainName::string(), TableName::string(), Cond::string())) ->
%%              {ok, Rec::list()} | {error, Msg::any()}
%% @end
%%--------------------------------------------------------------------
rget(DomainName, TableName, Cond) ->
    get_in(DomainName, TableName, Cond).

%%--------------------------------------------------------------------
%% @doc simple K/V store
%% @spec(store(Key::atom(), Value::any() ) -> 
%%              {ok, Cnt::integer()} | {partial, Cnt::integer()} | {ng, Msg::string}
%% @end
%%--------------------------------------------------------------------
store(Key, Value) ->
    store_in(Key, Value).

%%--------------------------------------------------------------------
%% @doc simple K/V store with cache enabled
%% @spec(cstore(Key::atom(), Value::any() ) -> 
%%              {ok, Cnt::integer()} | {partial, Cnt::integer()} | {ng, Msg::string}
%% @end
%%--------------------------------------------------------------------
cstore(Key, Value) ->
    cstore_in(Key, Value).

%%--------------------------------------------------------------------
%% @doc Range Query API
%%      KVList: [{FieldName::string(), Value::any()}]
%% @spec(rstore(DomainName::string(), TableName::string(), KVList)) ->
%%              {ok, Rec::list()} | {error, Msg::any()}
%% @end
%%--------------------------------------------------------------------
rstore(DomainName, TableName, KVList) ->
    store_in(DomainName, TableName, KVList).

%%--------------------------------------------------------------------
%% @doc Key::atom()|string()|integer()
%% @spec(del(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%% @end
%%--------------------------------------------------------------------
del(Key) ->
    del_in(Key).

%%--------------------------------------------------------------------
%% @doc Key::atom()|string()|integer()
%%      simple K/V delete with cache enabled
%% @spec(cdel(Key) -> {ok, {Key, Value::any()})} | {error, Msg)}
%% @end
%%--------------------------------------------------------------------
cdel(Key) ->
    cdel_in(Key).

%%--------------------------------------------------------------------
%% @doc Range Delete API
%%      Returned list does not include DTName
%% @spec(rdel(DomainName::string(), TableName::string(), Cond::string())) ->
%%              {ok, DeletedNum} | {error, Msg::any()}
%%
%% @end
%%--------------------------------------------------------------------
rdel(DomainName, TableName, Cond) ->
    del_in(DomainName, TableName, Cond).

%%--------------------------------------------------------------------
%% @doc return node specific module name
%% @spec
%% @end
%%--------------------------------------------------------------------
name(Name) when is_list(Name) -> 
    list_to_atom(?MODULE_NAME ++ "_" ++ Name);
name(Name) -> 
    list_to_atom(?MODULE_NAME ++ "_" ++ atom_to_list(Name)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(RegName) ->
    ets:new(?hm_global_table, [bag, public, named_table]),
    {ok, {RegName, [?hm_global_table]}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({register_table, TableName}, _From, {RegName, TableList}) ->
    ?info_p("register_table info:[~p] new table:[~p].~n", RegName, [TableList, TableName]),
    {reply, {ok, register_table, TableName}, {RegName, [TableName|TableList]}};

handle_call({unregister_table, TableName}, _From, {RegName, TableList}) ->
    ?info_p("unregister_table info:[~p] new table:[~p].~n", RegName, [TableList, TableName]),
    NewTableList = TableList -- [TableName],
    {reply, {ok, unregister_table}, {RegName, NewTableList}};

handle_call({get_table_info, DTName}, _From, {_RegName, TableList}=State) ->
    case lists:member(DTName, TableList) of
        true -> {reply, {ok, DTName}, State};
        false -> {reply, {error, no_table}, State}
    end;

handle_call({select_table, ?hm_global_table, DTName, FlistModified, MS}, _From, {_RegName, _TableList}=State) ->
    % ets:select(Tid, [{{'$1',['$2','$3','$4']},[{'and',{'==','$2',yyy},{'==', '$1', 'Domain1Tbl2'}}],['$$']}]).
    Reply = ets:select(?hm_global_table, [{{'$1',FlistModified},[{'and',{'==','$1',DTName},MS}], ['$_']}]),
    {reply, {ok, lists:usort(Reply)}, State};

handle_call({select_table, DTName, FlistModified, MS}, _From, State) ->
    Reply = ets:select(DTName, [{{'$1', FlistModified},MS,['$$']}]),
    {reply, {ok, lists:usort(Reply)}, State};

handle_call({select_delete_table, ?hm_global_table, DTName, FlistModified, MS}, _From, {_RegName, _TableList}=State) ->
    NumDeleted = ets:select_delete(?hm_global_table, [{{'$1',FlistModified},[{'and',{'==','$1',DTName},MS}], ['$_']}]),
    {reply, {ok, NumDeleted}, State};
handle_call({select_delete_table, DTName, FlistModified, MS}, _From, State) ->
    NumDeleted = ets:select_delete(DTName, [{{'$1', FlistModified},MS,['$$']}]),
    {reply, {ok, NumDeleted}, State};

handle_call({store, TableName, Key, Value}, _From, {RegName, TableList}) ->
    ?info_p("store:TableName:[~p] Key:[~p] Value:[~p] TableList:[~p].~n", store, [TableName, Key, Value, TableList]),
    case lists:member(TableName, TableList) of 
        false -> 
            {reply, {error, {store, no_table_found}}, {RegName, TableList}};
        true ->
            Ret = (catch ets:insert(TableName, {Key, Value})),
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

handle_call({delete, Key}, _From, {RegName, TableList}) ->
    ets:delete(?hm_global_table, Key),
    {reply, {ok, delete, Key}, {RegName, TableList}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------------------------------------------------------------------------
%% Internal Functions
%% ----------------------------------------------------------------------------

get_in(Key) ->
    {ok, TargetName, SuccListTarget} = hm_router:lookup_with_succlist(Key),
    SuccList = hm_misc:make_request_list(TargetName, SuccListTarget),
    get_from_succlist(SuccList, Key).

del_in(Key) ->
    {ok, TargetName, SuccListTarget} = hm_router:lookup_with_succlist(Key),
    SuccList = hm_misc:make_request_list(TargetName, SuccListTarget),
    del_from_succlist(SuccList, Key).

store_in(Key, Value) ->
    {ok, RouterName, SuccList} = hm_router:lookup_with_succlist(Key),
    store_in_to(RouterName, SuccList, ?hm_global_table, {Key, Value}).

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

cdel_in(Key) ->
    hm_cache:del_cache(Key),
    del_in(Key).

cstore_in(Key, Value) ->
    hm_cache:store_cache(Key, Value),
    store_in(Key, Value).

get_in(DomainName, TableName, Cond) ->
    DTName = list_to_atom(DomainName ++ TableName),
    %dynomite_prof:start_prof(make_request_list_from_dt),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    %dynomite_prof:stop_prof(make_request_list_from_dt),
    %dynomite_prof:start_prof(lookup_index_table_node),
    {ok, IndexTableNode, IndexDsNode} =  lookup_index_table_node(NodeList),
    %dynomite_prof:stop_prof(lookup_index_table_node),
    %dynomite_prof:start_prof(lookup_index_table_attribute),
    {ok, _Tid, AttList} =  lookup_index_table_attribute(IndexTableNode, DTName),
    %dynomite_prof:stop_prof(lookup_index_table_attribute),
    %dynomite_prof:start_prof(get_query_spec),
    {ok, MS, MSData, FlistIndex, FlistData} = get_query_spec(Cond, AttList),
    %dynomite_prof:stop_prof(get_query_spec),
    %dynomite_prof:start_prof(get_data_node_list),
    {ok, DataNodeList} = get_data_node_list(IndexDsNode, DTName, FlistIndex, MS, get),
    %dynomite_prof:stop_prof(get_data_node_list),
    %dynomite_prof:start_prof(scatter_gather),
    Res = scatter_gather(DomainName, DataNodeList, DTName, FlistData, MSData, TableName, Cond),
    %dynomite_prof:stop_prof(scatter_gather),
    Res.

del_in(DomainName, TableName, Cond) ->
    DTName = list_to_atom(DomainName ++ TableName),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    {ok, IndexTableNode, IndexDsNode} =  lookup_index_table_node(NodeList),
    {ok, _Tid, AttList} =  lookup_index_table_attribute(IndexTableNode, DTName),
    {ok, MS, MSData, FlistIndex, FlistData} = get_query_spec(Cond, AttList),

    %% get data node list from index table _AND_ delete index information
    {ok, DataNodeList} = get_data_node_list(IndexDsNode, DTName, FlistIndex, MS, delete),
    scatter_delete(DomainName, DataNodeList, DTName, FlistData, MSData, TableName, Cond).

store_in(DomainName, TableName, KVList) ->

    dynomite_prof:start_prof(make_request_list_from_dt),
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    dynomite_prof:stop_prof(make_request_list_from_dt),

    {IndexTableNode, _} = hd(NodeList), % index record is stored on this node in DTName table
    case hm_table:get_table_info(DomainName, TableName, NodeList) of
        {ok, DTName, AttList} ->

            %% Key is calcurated with DTName+values of key fields
            {ok, Key} = calc_key_from_key_data(DTName, KVList, AttList),

            %% data record is stored on this node in ?hm_global_table table
            %% at first data is stored, then index is stored
            {ok, DataTableNode, SuccList} = hm_router:lookup_with_succlist(Key), 
            case store_data(DTName, DataTableNode, SuccList, KVList) of
                {ok, _}      -> 
                    store_index(DTName, NodeList, DataTableNode, IndexTableNode, KVList, AttList);
                {partial, Cnt}      -> 
                    ?warning_p("store_data failed(partial) :DTName:[~p] DataTableNode:[~p] KVList:[~p] Cnt:[~p].~n", 
                        none, [DTName, DataTableNode, KVList, Cnt]),
                    store_index(DTName, NodeList, DataTableNode, IndexTableNode, KVList, AttList);
                {ng, Cnt} -> 
                    ?error_p("store_data failed :DTName:[~p] DataTableNode:[~p] KVList:[~p] Cnt:[~p].~n", 
                        none, [DTName, DataTableNode, KVList, Cnt]),
                    {error, store_into_datatable_failed}
            end;
        {error, ErrInfo} -> {error, ErrInfo}
    end.

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

get_data_node_list(IndexDsNode, DTName, FlistIndex, MS, Type) when Type =:= get ->
    {ok, RowList} = gen_server:call({global, IndexDsNode}, {select_table, DTName, FlistIndex, MS}),
    DataNodeList =
        lists:usort(
            lists:foldl(fun (Node, AccIn) ->
                            [hd(Node) | AccIn]
                        end, [], RowList)
        ),
    {ok, DataNodeList};
get_data_node_list(IndexDsNode, DTName, FlistIndex, MS, Type) when Type =:= delete ->
    {ok, RowList} = gen_server:call({global, IndexDsNode}, {select_table, DTName, FlistIndex, MS}),
    DataNodeList =
        lists:usort(
            lists:foldl(fun (Node, AccIn) ->
                            [hd(Node) | AccIn]
                        end, [], RowList)
        ),
    {ok, _DeletedNum} = gen_server:call({global, IndexDsNode}, {select_delete_table, DTName, FlistIndex, MS}),
    {ok, DataNodeList}.

lookup_index_table_attribute(IndexTableNode, DTNameTable) ->
    %% TODO: don't call gen_server:call of other module directly
    case gen_server:call({global, IndexTableNode}, {get_table_info, DTNameTable}) of
        {ok, TableName, AttList} -> {ok, TableName, AttList};
        {error, Msg} -> 
            {error, Msg}
    end.

scatter_delete(DomainName, DataNodeList, DTName, FlistData, MSData, TableName, Cond) ->
    Ref = make_ref(),

    % spawn a gathering process
    LoopPid = spawn_link( ?MODULE, gather_delete, [length(DataNodeList), self(), Ref, 0]),

    % spawn a scattering processes for each node
    lists:foreach(
           fun(El) -> 
                spawn_link(
                    ?MODULE, 
                    delete_data_table_solo, 
                    [El, DTName, FlistData, MSData, LoopPid, Ref]
                )
           end, 
           DataNodeList
    ),
    % receive total result
    receive
        {ok, Ref, DeletedNum} -> {ok, DeletedNum};
        {error, Ref, Msg} -> {error, Msg} 
    after ?TIMEOUT_GET ->
        ?error_p("get timeout :DomainName:[~p] TabName:[~p] Cond:[~p]~n",
                get, [DomainName, TableName, Cond]),
        {error, timeout}
    end.

gather_get(0, Pid, Ref, ResList) -> 
    ?info_p("gather_get OK:ResList:[~p]~n", gather_get, [ResList]),
    DataList = lists:usort(ResList),
    log_get_data(DataList),
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

gather_delete(0, Pid, Ref, Acc) -> 
    ?info_p("gather_delete OK:[~p]~n", gather_delete, [Acc]),
    Pid ! {ok, Ref, Acc};
gather_delete(Cnt, Pid, Ref, Acc) ->
    receive
        {ok, Ref, DeletedNum} ->
            gather_delete( Cnt - 1, Pid, Ref, Acc + DeletedNum );
        {error, Ref, Msg} ->
            ?error_p("gather_delete Error:Msg:[~p]~n", gather_delete, [Msg]),
            gather_delete( Cnt - 1, Pid, Ref, Acc )
    after ?TIMEOUT_GET ->
        Pid ! {error, Ref, timeout}
    end.

lookup_data_table_solo(NodeName, DTNameTable, FlistModified, MS, LoopPid, Ref) ->
    BareName = list_to_atom(atom_to_list(NodeName) -- ?PROCESS_PREFIX),
    DataNodeName = name(BareName),

    case hm_misc:is_alive(BareName) of
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

delete_data_table_solo(NodeName, DTNameTable, FlistModified, MS, LoopPid, Ref) ->
    BareName = list_to_atom(atom_to_list(NodeName) -- ?PROCESS_PREFIX),
    DataNodeName = name(BareName),

    case hm_misc:is_alive(BareName) of
        false -> 
            ?warning_p("DataNode Not Alive : Node:[~p].~n", none, [DataNodeName]),
            LoopPid ! {error, Ref, node_not_alive};
        true ->
            {ok, NumDeleted} = 
                gen_server:call(
                    {global, DataNodeName}, 
                    {select_delete_table, ?hm_global_table, DTNameTable, FlistModified, MS}
                ),
                LoopPid ! {ok, Ref, NumDeleted}
    end.

%% @doc returns hm_table/hm_ds name of the server first looked alive
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
store_data(DTName, DataTableNode, SuccList, KVList) ->
    VList = lists:map(fun({_Fld,Val}) -> Val end, KVList),
    store_in_to(DataTableNode, SuccList, ?hm_global_table, {DTName, VList}).
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
store_index(DTNameTable, SuccList, DataTableNode, IndexTableNode, KVList, AttList) ->
    {ok, Row} = extract_kv_tuples(KVList, AttList, true),
    Vlist = 
        lists:foldl(
            fun({_Fname,Value}, AccIn) ->
                    AccIn ++ [Value]
            end, [], Row),
    store_in_to(IndexTableNode, SuccList, DTNameTable, {DataTableNode, Vlist}).

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
                case lists:keysearch(Fname, 1, DataFields) of 
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
                                Data ++ AccIn;

                        ({_, Data},AccIn) when is_atom(Data) -> 
                                atom_to_list(Data) ++ AccIn;

                        ({_, Data},AccIn) when is_integer(Data) -> 
                                integer_to_list(Data) ++ AccIn
                      end, 
                      [], KeyList),
    {ok, list_to_atom(atom_to_list(DTName) ++ Key)}.

store_in_to(RouterName, SuccListTemp, TableName, {Key, Value}) ->
    % store to all successor list nodes
    SuccList = hm_misc:make_request_list(RouterName, SuccListTemp),
    ?info_p("store_to_succlist:SuccListTemp:[~p].~n", store, [SuccListTemp]),
    ?info_p("store_to_succlist:SuccList:[~p].~n", store, [SuccList]),
    ?info_p("DATA-STORE>>>>> Key:[~p] Value:[~p].~n", store, [Key, Value]),
    store_to_succlist(SuccList, TableName, Key, Value, {length(SuccList), 0}).

gather_store(0, Acc, Pid, Ref) -> Pid ! {ok, Ref, Acc};
gather_store(Len, Acc, Pid, Ref) ->
    receive 
        {ok, Ref, {ok, _Res}}    -> gather_store(Len - 1, Acc + 1, Pid, Ref);
        {ok, Ref, {error, _Res}} -> gather_store(Len - 1, Acc, Pid, Ref);
        {error, Ref, _Msg}       -> gather_store(Len - 1, Acc, Pid, Ref)
    after ?TIMEOUT_GET ->
        ?error_p("store timeout ~n", gather_store, []),
        Pid ! {error, Ref, timeout}
    end.

store_solo(RouterName, TableName, Key, Value, Pid, Ref) ->
     TargetName = name(atom_to_list(RouterName) -- ?PROCESS_PREFIX),
     case global:whereis_name(TargetName) of
         undefined ->
             ?error_p("store_to_succlist:Target undefined ~nKey:[~p] TargetName:[~p].~n",
                 store, [Key, TargetName]),
             Pid ! {error, Ref, undefined};
         _      ->
             ?info_p("store_to_succlist:Key:[~p] TargetName:[~p].~n", store, [Key, TargetName]),
             Reply = gen_server:call({global, TargetName}, {store, TableName, Key, Value}),
             Pid ! {ok, Ref, Reply}
     end.

store_to_succlist(SuccList, TableName, Key, Value, {Len, Cnt}) ->
    Ref = make_ref(),
    Pid = spawn_link(?MODULE, gather_store, [Len, Cnt, self(), Ref]),

    % spawn a scattering processes for each node
    lists:foreach(
           fun({El,_}) -> 
                spawn_link(
                    ?MODULE, 
                    store_solo, 
                    [El, TableName, Key, Value, Pid, Ref]
                )
           end, 
           SuccList
    ),
    % receive total result
    receive
        {ok, Ref, Acc} -> 
            case Acc =:= length(SuccList) of 
                true -> {ok, Acc};
                false -> {partial, Acc}
            end;
        {error, Ref, timeout} -> {ng, timeout}
    after ?TIMEOUT_GET ->
        ?error_p("store timeout :TabName:[~p] Key:[~p] Value:[~p]~n", store, [TableName, Key, Value]),
        {error, timeout}
    end.

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

del_from_succlist([], _Key) -> ok;
del_from_succlist(Succlist, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(atom_to_list(RouterName) -- ?PROCESS_PREFIX),
    case global:whereis_name(TargetName) of
        undefined ->
            ?warning_p("del:Key:[~p] TargetName:[~p] global name undefined .~n", delete, [Key, TargetName]);
        _ ->
            {ok, delete, Key} = gen_server:call({global, TargetName}, {delete, Key}),
            ?info_p("del:Key:[~p] TargetName:[~p].~n", delete, [Key, TargetName])
    end,
    del_from_succlist(tl(Succlist), Key).

log_get_data([]) -> ok;
log_get_data(DataList) ->
    ?info_p("DATA-GET  >>>>> Value:[~p].~n", store, [hd(DataList)]),
    log_get_data(tl(DataList)).
