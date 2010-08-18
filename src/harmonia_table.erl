-module(harmonia_table).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/1, stop/1, name/1, make_table/3, get_table_info/2 ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").


start_link(RegName) ->
    gen_server:start_link({local, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    gen_server:cast(name(RegName), stop).

terminate(_Reason, _State) -> ok.

init(RegName) ->
    {ok, {RegName, []}}.

%% spec(get_table_info(string(), string()) -> {ok, AttList::list()} |
%%                                            {error, no_node_available} | {error, no_table}
get_table_info(DomainName, TableName) ->
    NodeList = make_request_list(DomainName, TableName),
    case hm_misc:get_first_alive_entry(NodeList) of
        {error, none} -> {error, no_node_available};
        {NodeName, _Vector} -> 
            TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(NodeName)))),
            gen_server:call(TargetName, {get_table_info, DomainName, TableName})
    end.


%% spec(make_table(string(), string(), list()) -> {ok, list()}).
make_table(DomainName, TableName, AttList) ->
    NodeList = make_request_list(DomainName, TableName),

    % table is made on every node in successor list of correspondent node
    {ok, FailedList} = make_table_in(NodeList, [], DomainName, TableName, AttList),
    {ok, {NodeList, FailedList}}.

make_request_list(DomainName, TableName) ->
    % get atom() of node name
    NodeName = harmonia:lookup(DTName=list_to_atom(DomainName ++ TableName)),
    SuccList = gen_server:call(NodeName, copy_succlist),
    NodeList = hm_misc:make_request_list(NodeName, SuccList).



make_table_in([], FailedList, DomainName, TableName, AttList) ->
    {ok, FailedList};
make_table_in([{NodeName,_NodeVector}=CurNode|Tail], 
                  FailedList, DomainName, TableName, AttList) ->
    TargetName = name(list_to_atom(hm_misc:diff(?PROCESS_PREFIX, atom_to_list(NodeName)))),

    case hm_misc:is_alive(TargetName) of 
        true ->
            gen_server:call(TargetName, 
                {make_table, DomainName, TableName, AttList}),
            make_table_in(Tail, FailedList, 
                DomainName, TableName, AttList);
        false ->
            make_table_in(Tail, [CurNode|FailedList], 
                DomainName, TableName, AttList)
    end.


handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({get_table_info, DomainName, TableName}, _From, {RegName, TblList}=State) ->
    DTName=list_to_atom(DomainName ++ TableName),
    ReplyData = 
        case lists:keyfind(DTName, 2, TblList) of
            false -> {error, no_table};
            {_Tid, DTName, AttList} -> {ok, AttList}
        end,
    {reply, ReplyData, State};

handle_call({make_table, DomainName, TableName, AttList}, _From, {RegName, TblList}) ->
    DTName=list_to_atom(DomainName ++ TableName),
    TableId = ets:new(DTName, [ordered_set]),
    {reply, {ok, TableId}, {RegName, [{TableId, DTName, AttList}|TableName]}}.

name(Name) -> list_to_atom("harmonia_table_" ++ atom_to_list(Name)).
