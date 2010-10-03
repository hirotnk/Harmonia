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
%%% File    : hm_stabilizer.erl
%%% Description : this module is in charge of stabilization of the 
%%%               routing information in cooperation with hm_router
%%%               module
%%%
%%% control flow:
%%%  stabilize_loop(with check_pred) ----------->     sleep(stabilize_interval)
%%%         ^                                               |
%%%         |                                               |
%%%         |                                               v
%%%   sleep(fix_finger_interval)     <-----------      fix_finger_loop
%%%
%%%  Chord algorithm is based on the following paper:
%%%  [1] Stoica, I., Morris, R., Liben-Nowell, D., Karger, D. R., 
%%%      Kaashoek, M. F., Dabek, F., and Balakrishnan, H. 
%%%      2003. 
%%%      Chord: a scalable peer-to-peer lookup protocol for internet applications. 
%%%      IEEE/ACM Trans. Netw. 11, 1 (Feb. 2003), 17-32. 
%%%      DOI= http://dx.doi.org/10.1109/TNET.2002.808407  
%%%  
%%%  in this source, when reffered to as [1], it means above paper.
%%%
%%%
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro TANAKA <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_stabilizer).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_fsm).
-vsn('0.1').
%% API
-export([
        name/1,
        start_link/1, 
        stop/1 
        ]).
%% gen_fsm callbacks
-export([init/1, stabilize_loop/2, fixfinger_loop/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-include("harmonia.hrl").

%%====================================================================
%% API
%%====================================================================
start_link(RegName) ->
    gen_fsm:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    ?info_p("stop:stopping:[~p].~n", RegName, [RegName]),
    gen_fsm:send_event({global, name(RegName)}, stop).

name(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).

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
    {ok, stabilize_loop, RegName, ?stabilize_interval}.

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
%% @private
%% @doc
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
%%
%%
%% this routine is called periodically from hm_stabilizer
%% to maintain its successor by checking its current successor
%% that if it is the immediate predecessor of it
%% then notify it's successor of it
%%
%% algorithm: refer to [1]:
%%
%% 1. x = get pred of successor
%% 2. check if x is my immediage successor
%% 3. notify about me to successor
%% @end
%%--------------------------------------------------------------------
stabilize_loop(stop, RegName) ->
    {stop, normal, RegName};

stabilize_loop(timeout, RegName) ->

    % check predecessor - if it's dead, set nil to my pred
    hm_router:check_pred(RegName),

    % node is checked to be alive inside get_successor
    {ok, State} = hm_router:state_info(RegName),
    Succ = hm_misc:get_successor_alive(State),
    ?info_p("stabilize_loop:SuccName:[~p].~n", RegName, [Succ]),

    {NodeName, _NodeVector} = 
        case Succ of
            % successor is dead
            {error, successor_dead} -> 
                case hm_misc:get_first_alive_entry(tl(State#state.succlist)) of
                    {error, none} -> 
                        ?error_p("stabilize_loop:{error, none}.~n", RegName, []),
                        {State#state.node_name, State#state.node_vector};

                    % if successor is dead and there is some alive successor,
                    % build a new successor list
                    {NewSucc, NewSuccVector} -> 
                        ?info_p("stabilize_loop:NewSucc:[~p] NewSuccVector:[~p].~n", RegName, 
                               [NewSucc, NewSuccVector]),
                        make_succ_list({NewSucc, NewSuccVector}, hm_router:name(RegName)),

                        {NewSucc, NewSuccVector}
                end;

            % successor is alive
            {ok, {SuccName, SuccVector}} ->
                % need to always maintain(don't skip)
                make_succ_list({SuccName, SuccVector}, hm_router:name(RegName)),
                {SuccName, SuccVector};

            % currently no successor
            {error, no_successor} -> 
                {State#state.node_name, State#state.node_vector}
            
        end,

    % get predecessor of successor, and check if new successor exists
    % them, notify to "successor" about me
    %
    %  algorithm: refer to [1]:
    % 
    %  // called periodically. verifies n's immediate successor
    %  // and tells the successor about n
    %  n.stabilize()
    %    x = successor.predecessor
    %    if( x in (n, successor) )
    %      successor = x
    %    successor.notify(n)
    %
    check_new_successor(NodeName, RegName),
    gen_server:cast({global, NodeName}, {notify, {State#state.node_name, State#state.node_vector}}),

    {next_state, fixfinger_loop, RegName, ?stabilize_interval}.

%%--------------------------------------------------------------------
%% @doc this API refresh finger table periodically
%% 
%% algorithm: refer to [1]:
%%
%% // called periodically. refreshes finger table entries
%% // next stores the index of the next finger to fix
%% n.fix_fingers()
%%   next = next + 1
%%   if(next > m)
%%     next = 1
%%   finger[next]  = find_successor(n + 2^(next-1))
%%
%% @end
%%--------------------------------------------------------------------
fixfinger_loop(stop, RegName) ->
    {stop, normal, RegName};
fixfinger_loop(timeout, RegName) ->
    {ok, State} = hm_router:state_info(RegName),
    LVector = State#state.node_vector,
    Next = 
        case (State#state.current_fix + 1) > ?max_finger of
            true  -> 1;
            false -> State#state.current_fix + 1
        end,

    % get a fresh Next-th successor 
    NextVector = (LVector + round(math:pow(2, Next - 1))) rem ?max_key_value,
    NewSucc = hm_router:find_successor(RegName, NextVector, Next),

    % replace my finger's Next-th entry
    hm_router:fix_finger_set(RegName, Next, NewSucc, State#state.finger),

    ?info_p("fixfinger_loop: Next:[~p], State:[~p].~n", RegName, [Next, State]),
    {next_state, stabilize_loop, RegName, ?fixfinger_interval}.

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

check_new_successor(SuccName, RegName) ->
    hm_router:stabilize(RegName, hm_router:get_predecessor(SuccName)).

make_succ_list({SuccName, SuccVector}, MyNodeName) ->
    SuccList = [{SuccName, SuccVector} | hm_router:copy_succlist(SuccName)],
    NewSuccList = 
        case length(SuccList) > ?succ_list_len of
            true -> lists:sublist(SuccList, 1, ?succ_list_len);
            false -> SuccList
        end,
    ?info_p("stabilize_loop:Updated SuccList:[~p].~n", MyNodeName, [NewSuccList]),
    hm_router:set_succlist(MyNodeName, NewSuccList).
