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

-module(hm_table).
-behaviour(gen_server).
-vsn('0.1').

-export([
        create_table/3, 
        drop_table/2,
        get_table_info/2, 
        get_table_info/3,
        name/1, 
        start_link/1, 
        stop/1 
        ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").


start_link(RegName) ->
    gen_server:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    gen_server:cast({global, name(RegName)}, stop).

terminate(Reason, State) ->
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    ok.

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


%% spec(create_table(string(), string(), list()) -> {ok, {list(),list()}}).
create_table(DomainName, TableName, AttList) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    % table is made on every node in successor list of correspondent node
    {ok, FailedList} = create_table_in(NodeList, [], DomainName, TableName, AttList),
    {ok, {NodeList, FailedList}}.


%% @doc this module makes tables, and register table info to hm_ds process
create_table_in([], FailedList, _DomainName, _TableName, _AttList) ->
    {ok, FailedList};
create_table_in([{NodeName,_NodeVector}=CurNode|Tail], 
                  FailedList, DomainName, TableName, AttList) ->
    TargetName = name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
    case hm_misc:is_alive(TargetName) of 
        true ->
            {ok, DTName} =  gen_server:call({global, TargetName}, {create_table, DomainName, TableName, AttList}),
            TargetName_ds = hm_ds:name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
            case gen_server:call({global, TargetName_ds}, {register_table, DTName}) of
                {ok, register_table, DTName} ->
                    create_table_in(Tail, FailedList, DomainName, TableName, AttList);
                _Any ->
                    ?warning_p("register_table fail : TargetName_ds:[~p] DTName:[~p].~n", none, 
                        [TargetName_ds, DTName]),
                    create_table_in(Tail, [CurNode|FailedList], DomainName, TableName, AttList)
            end;
        false ->
            ?warning_p("Target Not Alive : Node:[~p].~n", none, [TargetName]),
            create_table_in(Tail, [CurNode|FailedList], DomainName, TableName, AttList)
    end.


%% spec(drop_table(string(), string(), list()) -> {ok, {list(),list()}}).
drop_table(DomainName, TableName) ->
    NodeList = hm_misc:make_request_list_from_dt(DomainName, TableName),

    % table is made on every node in successor list of correspondent node
    {ok, FailedList} = drop_table_in(NodeList, [], DomainName, TableName),
    {ok, {NodeList, FailedList}}.


drop_table_in([], FailedList, _DomainName, _TableName) ->
    {ok, FailedList};
drop_table_in([{NodeName,_NodeVector}=CurNode|Tail],
                  FailedList, DomainName, TableName) ->
    TargetName = name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
    case hm_misc:is_alive(TargetName) of 
        true ->
            case gen_server:call({global, TargetName}, {drop_table, DomainName, TableName}) of
                {ok, delete_table, DTName} -> 
                    TargetName_ds = hm_ds:name(list_to_atom( atom_to_list(NodeName) -- ?PROCESS_PREFIX )),
                    case gen_server:call({global, TargetName_ds}, {unregister_table, DTName}) of
                        {ok, unregister_table} ->
                            drop_table_in(Tail, FailedList, DomainName, TableName);
                        _Any -> 
                            drop_table_in(Tail, [CurNode|FailedList], DomainName, TableName)
                    end;
                _ -> drop_table_in(Tail, [CurNode|FailedList], DomainName, TableName)
            end;
        false ->
            drop_table_in(Tail, [CurNode|FailedList], DomainName, TableName)
    end.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({get_table_info, DomainName, TableName}, _From, {_RegName, TblList}=State) ->
    DTName=list_to_atom(DomainName ++ TableName),
    ReplyData = hm_misc:search_table_attlist(DTName, TblList),
    {reply, ReplyData, State};

handle_call({get_table_info, DTName}, _From, {_RegName, TblList}=State) ->
    ReplyData = hm_misc:search_table_attlist(DTName, TblList),
    {reply, ReplyData, State};

%% @doc index table is 
handle_call({create_table, DomainName, TableName, AttList}, _From, {RegName, TblList}) ->
    DTName=list_to_atom(DomainName ++ TableName),
    case ets:info(DTName) of
        undefined -> 
            DTName = ets:new(DTName, [bag, named_table, public]),
            {reply, {ok, DTName}, {RegName, [{DTName, AttList}|TblList]}};
        _ ->
            
            {reply, {error, already_exists}, {RegName, TblList}}
    end;

handle_call({drop_table, DomainName, TableName}, _From, {RegName, TblList}) ->
    DTName=list_to_atom(DomainName ++ TableName),
    case ets:info(DTName) of
        undefined -> 
            {reply, {error, not_exists}, {RegName, TblList}};
        _ ->
            ets:delete(DTName),
            NewState = lists:keydelete(DTName, 1, TblList),
            {reply, {ok, delete_table, DTName}, {RegName, NewState}}
    end.


name(Name) when is_list(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ Name);
name(Name) when is_atom(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).
