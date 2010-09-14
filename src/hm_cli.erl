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

-module(hm_cli).
-export([
        cget/1,
        cstore/2,
        drop_table/2,
        get/1,
        rget/3,
        get_node_names/0,
        get_table_info/2,
        log_start/0,
        log_stop/0,
        create_table/3,
        store/2,
        rstore/3
        ]).
-include_lib("eunit/include/eunit.hrl").

%% @spec(store(Key::atom()|string()|integer(), Value::any()) -> 
%%           {ok, Cnt::integer()} | 
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string}).
%%
%% @doc simple K/V store api
store(Key, Value) ->
    hm_ds:store(Key, Value).


%% @spec(store(Key::atom()|string()|integer(), Value::any()) ->
%%          {ok, Cnt::integer()} | 
%%          {partial, Cnt::integer()} |
%%          {ng, Msg::string}).
%%
%% @doc simple K/V store api with cache enabled
cstore(Key, Value) ->
    hm_ds:cstore(Key, Value).


%% @spec(rstore(DomainName::string(), TableName::string(), KVList::list()) -> 
%%           {ok, Cnt::integer()} |  
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string} |
%%           {error, Msg::string}).
%%
%% @doc store API for table format.
%%      this API stores index and data.
%%
%%      format of KVList::[{FldName::string(), Vaue::any()}]
%%      Note: table for this domain & table name, and field definition need to be
%%            made in advance.
rstore(DomainName, TableName, KVList) ->
    hm_ds:rstore(DomainName, TableName, KVList).

%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::Recset}|
%%                                              {error, nodata}).
%%
%% @doc Recset::Records, Records::ListofRec
%%      simple K/V get api
get(Key) ->
    hm_ds:get(Key).


%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::any}|
%%                                              {error, nodata}).
%%
%% @doc simple K/V get api with cache enabled
cget(Key) ->
    hm_ds:cget(Key).

%% @spec(rget(DomainName::string(), TableName::string(), Cond::string()) ->
%%           {ok, Result}|{error, Msg}).
%%
%% @doc rget API for table format.
%%      - format of Cond::string
%%      - available relational operator::==|!=|<|>|[=<|<=]|[>=|=>]
%%      - available logical operator::and|or
%%      - parenthesis also available::(|)
%%      - name of fields and tables need to be stringnum(start with character)
rget(DomainName, TableName, Cond) ->
    hm_ds:rget(DomainName, TableName, Cond).

%% @spec(get_node_names() -> {ok, NameList::list()}).
%%
%% @doc get list of nodes participating Harmonia ring
get_node_names() ->
    gen_server:call({global, hm_name_server}, get_name_list).


%% @spec(create_table(DomainName::string(), TableName::string(), AttList::list()) ->
%%           {ok, {NodeList::list(), FailedList::list()}}).
%%
%% @doc make table for storing index info of this table
%%      returns list of nodes in which tables were created, 
%%      and list of nodes which table creation failed
%%      TODO: this return value make sense??
create_table(DomainName, TableName, AttList) ->
    hm_table:create_table(DomainName, TableName, AttList).

%% @spec(drop_table(DomainName::string(), TableName::string()) ->
%%           {ok, {NodeList::list(), FailedList::list()}}).
%%
%% @doc drop index table
%%      returns list of nodes in which tables were created, 
%%      and list of nodes which table creation failed
%%      TODO: this return value make sense??
drop_table(DomainName, TableName) ->
    hm_table:drop_table(DomainName, TableName).


%% @spec(get_table_info(DomainName::string(), TableName::string()) ->
%%           {ok, Tid, AttList::list()} |
%%           {error, no_node_available} | {error, no_table}
%%
%% @doc returns Table Id to access it, and field attributes
%%      TODO: Tid should be known to users??
get_table_info(DomainName, TableName) ->
    hm_table:get_table_info(DomainName, TableName).

%% @spec(log_start() -> ok).
%%
%% @doc start logging for all nodes
log_start() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_start_in(NameList).

%% @spec(log_stop() -> ok).
%%
%% @doc stop logging for all nodes
log_stop() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_stop_in(NameList).

%% ----------------------------------------------------------------------------
%% Internal Functions
%% ----------------------------------------------------------------------------
log_start_in([]) -> ok;
log_start_in([{_Name, NodeName}|NodeList]) ->
    Res = rpc:call(NodeName, hm_event_mgr, add_file_handler, []),
    io:format("log start:[~p] Result:[~p]\n", [NodeName, Res]),
    log_start_in(NodeList).

log_stop_in([]) -> ok;
log_stop_in([{_Name, NodeName}|NameList]) ->
    Res = rpc:call(NodeName, hm_event_mgr, delete_file_handler, []),
    io:format("log stop:[~p] Result:[~p]\n", [NodeName, Res]),
    log_stop_in(NameList).

%% ----------------------------------------------------------------------------
%% EUnit Test Functions
%% ----------------------------------------------------------------------------

rangeq_test_() -> 
    [
        ?_assertEqual(hm_cli:store("Domain1", "Tbl2", [{"Fld1", xxx},{"Fld2", 32},{"Fld3", textfile1}])  ,{ok, 5}),
        ?_assertEqual(hm_cli:store("Domain1", "Tbl2", [{"Fld1", yyy},{"Fld2", 150},{"Fld3", textfile2}]) ,{ok, 5}),
        ?_assertEqual(hm_cli:store("Domain1", "Tbl2", [{"Fld1", zzz},{"Fld2", 3000},{"Fld3", textfile3}]),{ok, 5}),
        ?_assertEqual(hm_cli:store("Domain1", "Tbl2", [{"Fld1", aaa},{"Fld2", 9000},{"Fld3", textfile4}]),{ok, 5}),

        ?_assertEqual({ok, [[xxx,32,textfile1]]}   ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 32")),
        ?_assertEqual({ok, [[yyy,150,textfile2]]}  ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 150")),
        ?_assertEqual({ok, [[zzz,3000,textfile3]]} ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 3000")),
        ?_assertEqual({ok, [[aaa,9000,textfile4]]} ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 9000"))
    ].
