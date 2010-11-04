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
%%% @copyright (C) 2010, Yoshihiro TANAKA
%%% @doc
%%%  user interface of Harmonia
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_cli).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
%% API
-export([
        cget/1,
        create_table/3,
        cstore/2,
        drop_table/2,
        get/1,
        get_node_names/0,
        get_table_info/2,
        log_start/0,
        log_stop/0,
        rget/3,
        rstore/3,
        store/2,
        get_data_count/1 % only for test purpose
        ]).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc simple K/V store api
%% @spec(store(Key::atom()|string()|integer(), Value::any()) -> 
%%           {ok, Cnt::integer()} | 
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string}).
%% @end
%%--------------------------------------------------------------------
store(Key, Value) ->
    hm_ds:store(Key, Value).

%%--------------------------------------------------------------------
%% @doc simple K/V store api with cache enabled
%% @spec(store(Key::atom()|string()|integer(), Value::any()) ->
%%          {ok, Cnt::integer()} | 
%%          {partial, Cnt::integer()} |
%%          {ng, Msg::string}).
%%
%% @end
%%--------------------------------------------------------------------
cstore(Key, Value) ->
    hm_ds:cstore(Key, Value).

%%--------------------------------------------------------------------
%% @doc store API for range query
%%      this API stores index and data.
%%
%%      format of KVList::[{FldName::string(), Vaue::any()}]
%%      Note: table for this domain & table name, and field definition 
%%            need to be made in advance.
%% @spec(rstore(DomainName::string(), TableName::string(), KVList::list()) 
%%       -> 
%%           {ok, Cnt::integer()} |  
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string} |
%%           {error, Msg::string}).
%% @end
%%--------------------------------------------------------------------
rstore(DomainName, TableName, KVList) ->
    hm_ds:rstore(DomainName, TableName, KVList).

%%--------------------------------------------------------------------
%% @doc simple K/V get api
%%      Recset::Records, Records::ListofRec
%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::Recset}|
%%                                              {error, nodata}).
%% @end
%%--------------------------------------------------------------------
get(Key) ->
    hm_ds:get(Key).

%%--------------------------------------------------------------------
%% @doc simple K/V get api with cache enabled
%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::any}|
%%                                              {error, nodata}).
%%
%% @end
%%--------------------------------------------------------------------
cget(Key) ->
    hm_ds:cget(Key).

%%--------------------------------------------------------------------
%% @doc rget API for table format.
%%      - format of Cond::string
%%      - available relational operator::==|!=|<|>|[=<|<=]|[>=|=>]
%%      - available logical operator::and|or
%%      - parenthesis also available::(|)
%%      - name of fields and tables need to be stringnum(start with character)
%% @spec(rget(DomainName::string(), TableName::string(), Cond::string()) ->
%%           {ok, Result}|{error, Msg}).
%% @end
%%--------------------------------------------------------------------
rget(DomainName, TableName, Cond) ->
    hm_ds:rget(DomainName, TableName, Cond).

%%--------------------------------------------------------------------
%% @doc get list of nodes participating Harmonia ring
%% @spec(get_node_names() -> {ok, NameList::list()}).
%% @end
%%--------------------------------------------------------------------
get_node_names() ->
    hm_name_server:get_list().

%%--------------------------------------------------------------------
%% @doc make table for storing index info of this table
%%      returns list of nodes in which tables were created, 
%%      and list of nodes which table creation failed
%%      TODO: this return value make sense??
%% @spec(create_table(DomainName::string(), TableName::string(), AttList::list()) ->
%%           {ok, {NodeList::list(), FailedList::list()}}).
%% @end
%%--------------------------------------------------------------------
create_table(DomainName, TableName, AttList) ->
    hm_table:create_table(DomainName, TableName, AttList).

%%--------------------------------------------------------------------
%% @doc drop index table
%%      returns list of nodes in which tables were created, 
%%      and list of nodes which table creation failed
%%      TODO: this return value make sense??
%% @spec(drop_table(DomainName::string(), TableName::string()) ->
%%           {ok, {NodeList::list(), FailedList::list()}}).
%% @end
%%--------------------------------------------------------------------
drop_table(DomainName, TableName) ->
    hm_table:drop_table(DomainName, TableName).

%%--------------------------------------------------------------------
%% @doc returns Table Id to access it, and field attributes
%%      TODO: Tid should be known to users??
%% @spec(get_table_info(DomainName::string(), TableName::string()) ->
%%           {ok, Tid, AttList::list()} |
%%           {error, no_node_available} | {error, no_table}
%% @end
%%--------------------------------------------------------------------
get_table_info(DomainName, TableName) ->
    hm_table:get_table_info(DomainName, TableName).

%%--------------------------------------------------------------------
%% @doc start logging for all nodes
%% @spec(log_start() -> ok).
%% @end
%%--------------------------------------------------------------------
log_start() ->
    {ok, NameList} = hm_name_server:get_list(),
    log_start_in(NameList).

%%--------------------------------------------------------------------
%% @doc stop logging for all nodes
%% @spec(log_stop() -> ok).
%% @end
%%--------------------------------------------------------------------
log_stop() ->
    {ok, NameList} = hm_name_server:get_list(),
    log_stop_in(NameList).

%%--------------------------------------------------------------------
%% @doc get current data count
%% @spec(get_data_count() -> integer).
%% @end
%%--------------------------------------------------------------------
get_data_count(Name) ->
    hm_ds:get_data_count(Name).

%%%===================================================================
%%% Internal functions
%%%===================================================================
log_start_in([]) -> ok;
log_start_in([{_Name, NodeName}|NodeList]) ->
    Res = rpc:call(NodeName, hm_event_mgr, add_file_handler, []),
    io:format("log start:[~p] Result:[~p]\n", [NodeName, Res]),
    log_start_in(NodeList).

log_stop_in([]) -> ok;
log_stop_in([{_Name, NodeName}|NameList]) ->
    Res = rpc:call(NodeName, hm_event_mgr, delete_file_handler, []),
    case Res of 
       {error,module_not_found} ->
            io:format("log stop:[~p] : The file handler is not installed.\n", [NodeName]);
        _ ->
            io:format("log stop:[~p] Result:[~p]\n", [NodeName, Res])
    end,
    log_stop_in(NameList).

%%%===================================================================
%%% EUnit test functions
%%%===================================================================

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
