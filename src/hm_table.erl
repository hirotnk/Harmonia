-module(hm_table).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/1, stop/1, name/1, make_table/3, get_table_info/2, get_table_info/3 ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").


start_link(RegName) ->
    gen_server:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    gen_server:cast({global, name(RegName)}, stop).

terminate(_Reason, _State) -> ok.

init(RegName) ->
    {ok, {RegName, []}}.

%% spec(get_table_info(string(), string()) -> {ok, AttList::list()} |
%%                                            {error, no_node_available} | {error, no_table}
get_table_info(DomainName, TableName) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),
    get_table_info(DomainName, TableName, NodeList).

%% if you have nodelist to ask, use this API
get_table_info(DomainName, TableName, NodeList) ->
    case hm_misc:get_first_alive_entry(NodeList) of
        {error, none} -> {error, no_node_available};
        {NodeName, _Vector} -> 
            TargetName = name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
            gen_server:call({global, TargetName}, {get_table_info, DomainName, TableName})
    end.


%% spec(make_table(string(), string(), list()) -> {ok, {list(),list()}}).
make_table(DomainName, TableName, AttList) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    % table is made on every node in successor list of correspondent node
    {ok, FailedList} = make_table_in(NodeList, [], DomainName, TableName, AttList),
    {ok, {NodeList, FailedList}}.


make_table_in([], FailedList, DomainName, TableName, AttList) ->
    {ok, FailedList};
make_table_in([{NodeName,_NodeVector}=CurNode|Tail], 
                  FailedList, DomainName, TableName, AttList) ->
    TargetName = name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
    case hm_misc:is_alive(TargetName) of 
        true ->
            {ok, TableInfo} =  gen_server:call({global, TargetName}, {make_table, DomainName, TableName, AttList}),
            TargetName_ds = hm_ds:name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
            gen_server:call({global, TargetName_ds}, {register_table, TableInfo}),
            make_table_in(Tail, FailedList, DomainName, TableName, AttList);
        false ->
            make_table_in(Tail, [CurNode|FailedList], DomainName, TableName, AttList)
    end.


handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({get_table_info, DomainName, TableName}, _From, {RegName, TblList}=State) ->
    DTName=list_to_atom(DomainName ++ TableName),
    ReplyData = hm_misc:search_table_attlist(DTName, TblList),
    {reply, ReplyData, State};

handle_call({get_table_info, DTName}, _From, {RegName, TblList}=State) ->
    ReplyData = hm_misc:search_table_attlist(DTName, TblList),
    {reply, ReplyData, State};

handle_call({make_table, DomainName, TableName, AttList}, _From, {RegName, TblList}) ->
    DTName=list_to_atom(DomainName ++ TableName),
    TableId = ets:new(DTName, [duplicate_bag, public]),
    {reply, {ok, {DTName, TableId}}, {RegName, [{TableId, DTName, AttList}|TblList]}}.

name(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).
