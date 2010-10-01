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

-module(hm_cache_mgr).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_fsm).
-vsn('0.1').

-export([
        cache_cleanup_lru/2,
        start_link/1, 
        stop/1 
        ]).
-export([init/1, terminate/3]).

-include("harmonia.hrl").
-include_lib("include/ms_transform.hrl").


start_link(RegName) ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, RegName, []).

stop(RegName) ->
    ?info_p("stop:stopping:[~p].~n", RegName, [RegName]),
    gen_fsm:send_event({global, name(RegName)}, stop).

terminate(Reason, StateName, State) ->
    ?info_p("terminate:Reason:[~p] StateName:[~p], State:[~p]~n", none, [Reason, StateName, State]),
    ok.

cache_cleanup_lru(timeout, RegName) ->
    RowCnt = ets:select_count(?hm_ets_cache_table, ets:fun2ms(fun(_)->true end)),
    case RowCnt > ?ets_cache_threshold_num of
        true -> 
            ?info_p("Current Num:[~p]\n", RegName, [RowCnt]),
            clean_up_worker(RowCnt - ?ets_cache_threshold_num);
        false -> ok
    end,
    {next_state, cache_cleanup_lru, RegName, ?cache_cleanup_interval}.

clean_up_worker(ToDelCnt) ->
    {Megasec, Sec, _Microsec} = now(),
    clean_up_worker_in(0, Megasec*1000000 + Sec, ToDelCnt).

clean_up_worker_in(CurCnt, CurTimesec, ToDelCnt) ->
    %% first condition: reference count == count & expired
    MS = ets:fun2ms(
            fun({K,_,Cnt,Timesec}) when Cnt =:= CurCnt, Timesec < CurTimesec ->
                K
            end
         ),
    NextDelCnt =
        case ets:select(?hm_ets_cache_table, MS) of
            [] ->
                %% second condition: reference count == count or expired
                MS2 = ets:fun2ms(
                        fun({K,_,Cnt,Timesec}) when Cnt =:= CurCnt; Timesec < CurTimesec ->
                            K
                        end
                      ),
                case ets:select(?hm_ets_cache_table, MS2) of
                    %% check with next reference count(count+1)
                    [] -> ToDelCnt;
                    RecMatch2 -> 
                        %% delete ToDelCnt records from these records 
                        del_until_threshold(RecMatch2, ToDelCnt)
                end;
            %% delete ToDelCnt records from these records 
            RecMatch -> del_until_threshold(RecMatch, ToDelCnt)
        end,
    RowCnt = ets:select_count(?hm_ets_cache_table, ets:fun2ms(fun(_)->true end)),
    ?info_p("New Row Num:[~p]\n", none, [RowCnt]),
    case NextDelCnt > 0 of
        true -> 
            %% there is still need to delete some records
            clean_up_worker_in(CurCnt+1, CurTimesec, NextDelCnt);
        false -> 
            ok
    end.

del_until_threshold([], ToDelCnt) -> ToDelCnt;
del_until_threshold(_Any, 0) -> 0;
del_until_threshold([Key|RecList], ToDelCnt) ->
    ets:delete(?hm_ets_cache_table, Key),
    del_until_threshold(RecList, ToDelCnt - 1).

init(RegName) -> 
    hm_cache:start(),
    {ok, cache_cleanup_lru, RegName, ?cache_cleanup_interval}.

name(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).

