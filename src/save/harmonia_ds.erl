-module(harmonia_ds).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/1, stop/1, store/2, store/3, get/1, name/1, table_name/1,
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

terminate(_Reason, _State) ->
    hm_misc:crypto_stop(),
    ok.

create_bag(BagName) ->
    store(?hm_global_table, BagName).

put(BagName, Key, Value) ->
    NodeName = harmonia:lookup(Key),
    store(BagName, BagName, {Key, NodeName}),
    store(BagName, Key, {Key, Value}).

get(BagName, Key, Cond) ->
    TargetName = harmonia:lookup(BagName),
    SuccList = gen_server:call(TargetName, copy_succlist),

    % return list of { key, nodename }
    NodeList = get_from_succlist(SuccList, BagName, Key, Cond), 

    % ask every node in parallel
    issue_retrieval(BagName, NodeList, []).


issue_retrieval(BagName, [], Acc) -> Acc;
issue_retrieval(BagName, NodeList, Acc) ->
    {Key, NodeName} = hd(NodeList),

    SuccList = gen_server:call(NodeName, copy_succlist),
    Result = get_from_succlist(SuccList, BagName, Key),
    issue_retrieval(BagName, tl(NodeList), [Result|Acc]).



get(Key) ->
    TargetName = harmonia:lookup(Key),
    SuccList = gen_server:call(TargetName, copy_succlist),

    get_from_succlist(SuccList, Key).


store(Key, Value) ->
    store_in(?hm_global_table, Key, Value).

store(TableId, Key, Value) ->
    store_in(TableId, Key, Value).

store_in(TableId, Key, Value) ->
    % lookup the node that is in charge of the key
    RouterName = harmonia:lookup(Key),
    ?debug_print("store:Key:[~p] RouterName:[~p].~n", store, [Key, RouterName]),

    % store to all successor list nodes
    SuccList = gen_server:call(RouterName, copy_succlist),
    ?debug_print("store:SuccList:[~p] RouterName:[~p].~n", store, [SuccList, RouterName]),
    store_to_succlist([{RouterName,instance}|tl(SuccList)], 
        TableId, Key, Value, {?succ_list_len, 0}).


% the successor list here includes target node itself, and
% tail of successor list
store_to_succlist([], TableId, Key, Value, {Len, Cnt}) -> 
    ?debug_print("store_to_succlist:LINE:[~p] Tab:[~p] Key:[~p] Value:[~p] count:[~p].~n", 
        store, [?LINE, TableId, Key, Value, {Len, Cnt}]),
    case Len =:= Cnt of
        true -> {ok, Cnt};
        false -> 
            case Cnt =:= 0 of
                true -> {ng, Cnt};
                false -> {partial, Cnt}
            end
    end;
store_to_succlist(SuccList, TableId, Key, Value, {Len, Cnt}) ->
    {RouterName, _} = hd(SuccList),

    TargetName = name(list_to_atom(hm_misc:diff("harmonia_", atom_to_list(RouterName)))),
    case whereis(TargetName) of 
        undefined -> 
            % TODO: when fail to store data, or node is down, write logging to  
            %       another file and continue processing
            ?debug_print("***ERROR*** store_to_succlist:Target undefined ~nKey:[~p] TargetName:[~p].~n", 
                store, [Key, TargetName]),
            NewCnt = Cnt;
        _Pid      -> 
            ?debug_print("store_to_succlist:Key:[~p] TargetName:[~p].~n", store, [Key, TargetName]),
            Reply = gen_server:call(TargetName, {store, TableId, Key, Value}),
            case Reply of
                true -> NewCnt = Cnt + 1;
                false -> NewCnt = Cnt
            end
    end,
    store_to_succlist(tl(SuccList), TableId, Key, Value, {Len, NewCnt}).

%% TODO: clean this interfaces of get_from_succlist
get_from_succlist([], _Key) -> {error, nodata};
get_from_succlist(Succlist, Key) ->
    {RouterName, _} = hd(Succlist),
    TargetName = name(list_to_atom(hm_misc:diff("harmonia_", atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), Key);
        _ ->
            Result = gen_server:call(TargetName, {get, Key}),
            ?debug_print("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
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
    TargetName = name(list_to_atom(hm_misc:diff("harmonia_", atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), BagName, Key);
        _ ->
            Result = gen_server:call(TargetName, {get, BagName, Key}),
            ?debug_print("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
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
    TargetName = name(list_to_atom(hm_misc:diff("harmonia_", atom_to_list(RouterName)))),
    case whereis(TargetName) of
        undefined ->
            get_from_succlist(tl(Succlist), BagName, Key, Cond);
        _ ->
            % get list of [{key, nodename},..]
            Result = gen_server:call(TargetName, {get, BagName, Key, Cond}),

            ?debug_print("get:Key:[~p] TargetName:[~p] Result:[~p] .~n", get, [Key, TargetName, Result]),
            case length(Result) of
                0 -> 
                    get_from_succlist(tl(Succlist), BagName, Key, Cond);
                _ -> 
                    Result
            end
    end.

init(RegName) ->
    GlobalTableId = ets:new(?hm_global_table, [ordered_set]),
    {ok, {RegName, [{?hm_global_table, GlobalTableId}]}}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({store, TableId, Key, Value}, _From, {RegName, GlobalTableId}) ->
    Ret = ets:insert(TableId, {Key, Value}),
    {reply, Ret, {RegName, GlobalTableId}};

handle_call({get, Key}, _From, {RegName, GlobalTableId}) ->
    Reply = ets:lookup(GlobalTableId, Key),
    {reply, Reply, {RegName, GlobalTableId}};

handle_call({get, TableId, Key}, _From, {RegName, GlobalTableId}) ->
    Reply = ets:lookup(TableId, Key),
    {reply, Reply, {RegName, GlobalTableId}};

handle_call({get, BagName, Key, Cond={Op,Val}}, _From, {RegName, GlobalTableId}) ->
    % list of {key, nodename}
    Reply = ets:select(BagName,[{ {'$1','$2'}, [ { Op, '$1', Val } ],['$_'] }]),
    {reply, Reply, {RegName, GlobalTableId}}.

name(Name) -> list_to_atom("harmonia_ds_" ++ atom_to_list(Name)).

table_name(Name) -> list_to_atom("table_" ++ atom_to_list(Name)).

