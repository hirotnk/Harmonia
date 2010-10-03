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
%%% @doc library module
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro TANAKA <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_misc).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-vsn('0.1').
%% API
-export([
        check_exist/2,
        check_pred_and_successor/1,
        crypto_start/0,
        crypto_stop/0,
        del_dup/1,
        get_digest/1,
        get_first_alive_entry/1,
        get_first_fit_router/1,
        get_node/0,
        get_rand_procname/0,
        get_successor_alive/1,
        get_vector_from_name/2,
        is_alive/1,
        is_pred_nil/1,
        make_log_file_name/0,
        make_request_list/2,
        make_request_list_from_dt/2,
        replace_nth/3,
        search_table_attlist/2
        ]).

-include("harmonia.hrl").

%%%===================================================================
%%% API
%%%===================================================================
get_successor_alive(State) when length(State#state.finger) > 0 ->
    {SuccName, SuccVector} = hd(State#state.finger),
    % check if the Succ is alive 
    BareName = list_to_atom(atom_to_list(SuccName) -- ?PROCESS_PREFIX),
    case is_alive(BareName) of
        true -> {ok, {SuccName, SuccVector}};
        false -> {error, successor_dead}
    end;
get_successor_alive(_State) -> {error, no_successor}.

get_vector_from_name(_RegName, []) -> {error, instance};
get_vector_from_name(RegName, Finger) ->
    {Name, Vector} = hd(Finger),
    case (Name =:= RegName) of 
        true -> {ok, Vector};
        false -> get_vector_from_name(RegName, tl(Finger))
    end.

get_node() ->
    atom_to_list(node()).

get_rand_procname() ->
    % bypassing get_name_list API for performance
    NameList = global:registered_names(), 
    {ok, Name} = get_first_fit_router(NameList), % clash, if error.
    {ok, Name}.

get_first_fit_router([]) -> error;
get_first_fit_router([Candidate|NameList]) ->
    case lists:prefix("hm_router_", atom_to_list(Candidate)) of
        true -> {ok, Candidate};
        false ->
            get_first_fit_router(NameList)
    end.

get_first_alive_entry([]) -> {error, none};
get_first_alive_entry([{Name, _Vector} = FirstNode|NodeList]) ->

    BareName = list_to_atom(atom_to_list(Name) -- ?PROCESS_PREFIX),
    case is_alive(BareName) of
        true  -> FirstNode;
        false ->
            get_first_alive_entry(NodeList)
    end.

is_alive(BareName) ->
    % 'foo', 'hm_router_foo', both type of name is available,
    % but need to specify 'foo' type name in case it's dead
    % to remove from name server
    case global:whereis_name(BareName) of
        undefined -> 
            hm_name_server:unregister(BareName),
            false;
        _ -> true
    end.

check_exist(_AppName, []) -> false;
check_exist(AppName, [{AppName,_,_}|_Applist]) -> true;
check_exist(AppName, Applist) -> 
    check_exist(AppName, tl(Applist)).

crypto_start() ->
    Apps = application:which_applications(),
    case check_exist(crypto, Apps) of
        true  -> {ok, exists};
        false -> crypto:start(), {ok, started}
    end.

crypto_stop() ->
    Apps = application:which_applications(),
    case check_exist(crypto, Apps) of
        true  -> crypto:stop(), {ok, stopped};
        false -> {ok, not_exists}
    end.

replace_nth(N, Val, List) ->
    replace_nth_in(1, N, length(List), Val, List, []).

check_pred_and_successor(State) ->
    % includes info about predecessor also
    case State#state.predecessor =:= nil of 
        true ->
            Pred = pred_is_nil;
        false ->
            Pred = pred_is_not_nil
    end,
    case length(State#state.finger) > 0 of
        true  -> {succ_exists, Pred};
        false -> {no_succ_exists, Pred}
    end.

is_pred_nil(State) ->
    case State#state.predecessor =:= nil of
        true -> true;
        false -> false
    end.

make_request_list(TargetName, SuccListTarget) ->
    % this order is important, don't sort !!
    % also, if elements of list duplicate, system ends up with trying same
    % operation multiple times, so this list must have uniq elements
    del_dup([{TargetName, instance} | SuccListTarget ]).

make_request_list_from_dt(DomainName, TableName) ->
    {ok, NodeName, SuccList} = hm_router:lookup_with_succlist(list_to_atom(DomainName ++ TableName)),
    make_request_list(NodeName, SuccList).


get_digest(Key) when is_integer(Key) ->
    <<Vector:160>> = crypto:sha(integer_to_list(Key)),
    Vector rem ?max_key_value;
get_digest(Key) when is_atom(Key) ->
    <<Vector:160>> = crypto:sha(atom_to_list(Key)),
    Vector rem ?max_key_value;
get_digest(Key) when is_list(Key) ->
    <<Vector:160>> = crypto:sha(Key),
    Vector rem ?max_key_value.

search_table_attlist(DTName, TblList) ->
    case lists:keysearch(DTName, 1, TblList) of
        false -> {error, no_table};
        {value, {DTName, AttList}} -> {ok, DTName, AttList}
    end.

del_dup(List) ->
    lists:reverse(del_dup_in(List)).

make_log_file_name() ->
    {ok, {_, Name}}    = hm_config:get(name),
    {ok, {_, Logfile}} = hm_config:get(logfile),
    {ok, {_, Ext}}     = hm_config:get(logfile_ext),
    {ok, {_, Logdir}}  = hm_config:get(logdir),
    Logdir ++ Logfile ++ "_" ++ atom_to_list(Name) ++ Ext.

%%%===================================================================
%%% Internal functions
%%%===================================================================
replace_nth_in(C, N, Len, _Val, _Old, New) when (C > N) and (C > Len) -> 
    lists:reverse(New);
replace_nth_in(C, C, Len, Val, Old, New) ->
    case (length(Old) > 0) of
        true -> replace_nth_in(C+1, C, Len, Val, tl(Old), [Val|New]);
        false -> replace_nth_in(C+1, C, Len, Val, [], [Val|New])
    end;
replace_nth_in(C, N, Len, Val, [], New) ->
    replace_nth_in(C+1, N, Len, Val, [], [nil|New]);
replace_nth_in(C, N, Len, Val, Old, New) ->
    case (length(Old) > 0) of
        true  -> replace_nth_in(C+1, N, Len, Val, tl(Old), [hd(Old)|New]);
        false -> replace_nth_in(C+1, N, Len, Val, [], [hd(Old)|New])
    end.
   
del_dup_in(Old) ->
    lists:foldl(
        fun({Node,Vec}, Acc) -> 
            case lists:keysearch(Node, 1, Acc) of
                false      -> [{Node,Vec}|Acc];
                {value, _} -> Acc
            end
        end, [], Old
    ).
