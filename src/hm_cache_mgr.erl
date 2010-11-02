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
%%% File    : hm_cache_mgr.erl
%%% Description : periodically maintain local cache with LRU manner
%%%
%%% (1) cache_cleanup_lru runs periodically, check if the number of
%%%     records exceeds the threshold of cache, if true, go to (2)
%%%     if false, sleep for configured period of time
%%%
%%% (2) perform the cleaning with following logic:
%%%       referenced count and expiration time is to be considered:
%%%
%%%     CheckCnt = 0
%%%     cleaned_record_num = 0
%%%     to_be_cleaned_num = total_cache_recnum - threshold_cache
%%%
%%%     while( cleaned_record_num < to_be_cleaned_num ){
%%%         Recset = select records where reference count == CheckCnt
%%%         while( cleaned_record_num < to_be_cleaned_num ){
%%%             if Recset.rec.expiration_time > now() 
%%%                 delete Recset.rec
%%%                 cleaned_record_num++
%%%             end
%%%             if no more record in Recset then
%%%                 break
%%%             end
%%%         }
%%%         CheckCnt++
%%%     }
%%%-------------------------------------------------------------------
-module(hm_cache_mgr).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_fsm).
-vsn('0.1').
%% API
-export([
        start_link/1,
        stop/1 
        ]).
%% gen_fsm callbacks
-export([init/1, cache_cleanup_lru/2,handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-include("harmonia.hrl").
-include_lib("include/ms_transform.hrl").

%%====================================================================
%% API
%%====================================================================
start_link(RegName) ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, RegName, []).

stop(RegName) ->
    ?info_p("stop:stopping:[~p].~n", RegName, [RegName]),
    gen_fsm:send_event({global, name(RegName)}, stop).

%%====================================================================
%% gen_fsm callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, StateName, State} |
%%                         {ok, StateName, State, Timeout} |
%%                         ignore                              |
%%                         {stop, StopReason}
%% Description:Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/3,4, this function is called by the new process to
%% initialize.
%%--------------------------------------------------------------------
init(RegName) -> 
    hm_cache:start(),
    {ok, cache_cleanup_lru, RegName, ?cache_cleanup_interval}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, StateName, State) ->
    ?info_p("terminate:Reason:[~p] StateName:[~p], State:[~p]~n", none, [Reason, StateName, State]),
    ok.

%%--------------------------------------------------------------------
%% Function:
%% state_name(Event, State) -> {next_state, NextStateName, NextState}|
%%                             {next_state, NextStateName,
%%                                NextState, Timeout} |
%%                             {stop, Reason, NewState}
%% Description:There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same name as
%% the current state name StateName is called to handle the event. It is also
%% called if a timeout occurs.
%%--------------------------------------------------------------------
cache_cleanup_lru(timeout, RegName) ->
    RowCnt = ets:select_count(?hm_ets_cache_table, ets:fun2ms(fun(_)->true end)),
    case RowCnt > ?ets_cache_threshold_num of
        true -> 
            ?info_p("Current Num:[~p]\n", RegName, [RowCnt]),
            clean_up_worker(RowCnt - ?ets_cache_threshold_num);
        false -> ok
    end,
    {next_state, cache_cleanup_lru, RegName, ?cache_cleanup_interval}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
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

name(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).
