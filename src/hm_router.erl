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
%%% @doc Chord base router
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
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro TANAKA <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_router).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_server).
-vsn('0.1').

%% API
-export([
        check_pred/1,
        get_predecessor/1,
        stabilize/2,
        copy_succlist/1,
        fix_finger_set/4,
        set_succlist/2,
        find_successor/3,
        lookup/1, 
        lookup_with_succlist/1,
        name/1,
        start/1, 
        start_link/1,
        state_info/1, 
        state_info/2, 
        stop/0, 
        stop/1 
        ]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("harmonia.hrl").

%%%===================================================================
%%% API
%%%===================================================================
start([]) -> ok;
start(NodeNameList) ->
    start_link(hd(NodeNameList)),
    start(tl(NodeNameList)).

start_link({create, RegName}) ->
    gen_server:start_link( {global, name(RegName)}, ?MODULE, {create, RegName}, []);
start_link({join, RegName, RootName}) ->
    gen_server:start_link( {global, name(RegName)}, ?MODULE, {{join, RootName}, RegName}, []).

stop() ->
    gen_server:cast(?MODULE, stop).

stop(RegName) ->
    gen_server:cast({global, name(RegName)}, stop).

%%--------------------------------------------------------------------
%% @spec(lookup(Key::atom()) -> atom()).
%% @doc returns successor name in the form of harmonia_foo(bare name)
%% @end
%%--------------------------------------------------------------------
lookup(Key) ->
    KeyVector = hm_misc:get_digest(Key),
    {ok, RegName} = hm_misc:get_rand_procname(),
    {SuccName, _} = gen_server:call({global, RegName}, {find_successor, KeyVector, nil}),
    SuccName.

%%--------------------------------------------------------------------
%% @spec(lookup_with_succlist(Key::atom()) -> {ok, atom(), SuccList::list()}).
%% @doc returns successor name with its successor list
%% @end
%%--------------------------------------------------------------------
lookup_with_succlist(Key) ->
    KeyVector = hm_misc:get_digest(Key),
    {ok, RegName} = hm_misc:get_rand_procname(),
    {{SuccName, _}, SuccList} = 
        gen_server:call({global, RegName}, {find_successor_with_succlist, KeyVector, nil}),
    {ok, SuccName, SuccList}.

%%--------------------------------------------------------------------
%% @spec(find_successor(RegName::atom(), NodeVector::integer(),
%%                      Current::integer()) -> Successor).
%% @doc this routine is mainly called by hm_stabilizer modle
%% @end
%%--------------------------------------------------------------------
find_successor(RegName, NodeVector, Current) ->
    gen_server:call({global, hm_router:name(RegName)}, {find_successor, NodeVector, Current}).

state_info(RegName) ->
    gen_server:call({global, name(RegName)}, state_info).
state_info(RegName, NodeName) ->
    gen_server:call({name(RegName), NodeName}, state_info).

check_pred(RegName) ->
    gen_server:cast({global, hm_router:name(RegName)}, check_pred).

get_predecessor(SuccName) ->
    gen_server:call({global, SuccName}, get_predecessor).

copy_succlist(SuccName) ->
    gen_server:call({global, SuccName}, copy_succlist).

set_succlist(MyNodeName, NewSuccList) ->
    gen_server:cast({global, MyNodeName}, {set_succlist, NewSuccList}).

stabilize(RegName, PredOfSucc) ->
    gen_server:cast({global, hm_router:name(RegName)}, {stabilize, PredOfSucc}).

%%--------------------------------------------------------------------
%% @spec(fix_finger_set(RegName::atom(), Current::integer(), NewSucc, Finger) -> ok).
%% @doc replace nth entry in finger list
%% @end
%%--------------------------------------------------------------------
fix_finger_set(RegName, Current, NewSucc, Finger) ->
    gen_server:cast({global, hm_router:name(RegName)}, {fix_finger, Current, NewSucc, Finger}),
    ok.

name(Name) -> list_to_atom("hm_router_" ++ atom_to_list(Name)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Op, RegName}) ->
    hm_misc:crypto_start(),
    NodeName = name(RegName),
    NodeVector = hm_misc:get_digest(NodeName),
    State = #state{node_name = NodeName, node_vector = NodeVector},
    NewState = 
        case Op of 
            create -> 
                State#state{finger = [{NodeName, NodeVector}]};
            {join, RootNodeName} ->
                NewSucc = gen_server:call(
                              {global, name(RootNodeName)}, 
                              {find_successor, NodeVector, nil}
                          ),
                State#state{finger = [NewSucc]}
        end,
    {ok, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    hm_misc:crypto_stop(),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast({return_with_succlist, From}, State) ->
    gen_server:reply(From, {{State#state.node_name, State#state.node_vector}, State#state.succlist}),
    {noreply, State};

handle_cast({find_successor_ask_other, Key, NodeVector, From}, State) ->
    % The flow comes here means that it was forwarded to this node
    find_successor_ask_other(Key, NodeVector, From, State),
    {noreply, State};

handle_cast({fix_finger, Next, NewSuccessor, Finger}, State) ->
    {noreply, State#state{finger = hm_misc:replace_nth(Next, NewSuccessor, Finger)}};

handle_cast({set_succlist, SuccList}, State) ->
    {noreply, State#state{succlist = SuccList}};

handle_cast({notify, NodeInfo}, State) ->
    {noreply, notify(NodeInfo, State)};

handle_cast({stabilize, PredOfSucc}, State) ->
    {noreply, stabilize_in(PredOfSucc, State)};

handle_cast(check_pred, State) ->
    {noreply, check_pred_in(State)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({find_successor, NodeVector, CurrentFix}, From, State) ->
    find_successor_handle_call_in(find_successor, NodeVector, CurrentFix, From, State);

handle_call({find_successor_with_succlist, NodeVector, CurrentFix}, From, State) ->
    find_successor_handle_call_in(find_successor_with_succlist, NodeVector, CurrentFix, From, State);

handle_call(copy_succlist, _From, State) ->
    {reply, State#state.succlist, State};

handle_call(get_predecessor, _From, State) ->
    {reply, State#state.predecessor, State};

handle_call(get_successor, _From, State) ->
    {reply, hm_misc:get_successor_alive(State), State};

handle_call(state_info, _From, State) ->
    {reply, {ok, State}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc this API is called by handle_call immediately,
%%      and forwarding to other nodes starts from this point
%% 
%% algorithm: refer to [1]:
%% // ask node n to find the successor of id
%% n.find_successor(id)
%%   if(id in (n, successor])
%%     return successor
%%   else
%%     n' = closest_preceding_node(id)
%%     return n'.find_successor(id)
%%
%% @end
%%--------------------------------------------------------------------
find_successor_handle_call_in(Handlecall_Key, NodeVector, CurrentFix, From, State) ->
    NewState = 
        case CurrentFix of
            nil -> State; % inquired by other, no change to current_fix
            _   -> State#state{current_fix = CurrentFix} % inquired by self-stabilizer
        end,
    case NodeVector =:= State#state.node_vector of

        % when node vector == current node, return current node
        true ->
            case Handlecall_Key =:= find_successor_with_succlist of
                true  -> {reply, {{State#state.node_name, State#state.node_vector}, State#state.succlist}, NewState};
                false -> {reply, {State#state.node_name, State#state.node_vector}, NewState}
            end;

        false ->
            return_successor_info(
                Handlecall_Key, 
                find_successor_in(NodeVector, State),
                NodeVector,
                From,
                NewState
            )
    end.

%%--------------------------------------------------------------------
%% @spec(find_successor_in(NodeVector, State) -> {found_me, {Succ, SuccVector}} |
%%                                               {found_other, TargetNode} |
%%                                               {not_found, Node}.
%% @doc this API seaches the routing information for the 'NodeVector' in this
%%      node's 'State' and returns 3 types of judgements described above
%% @end
%%--------------------------------------------------------------------
find_successor_in(NodeVector, State) ->
    {Succ, SuccVector} =
        case hm_misc:get_first_alive_entry(State#state.finger) of
            {error, none} -> 
                {State#state.node_name, State#state.node_vector};
            {TmpSucc, TmpSuccVector} -> 
                {TmpSucc, TmpSuccVector}
        end,
    case is_between2(State#state.node_vector, NodeVector, SuccVector) of
        % my successor is in charge of the node vector
        true  -> 
            {found_other, {Succ, SuccVector}};

        % my successor is not the successor of NodeVector,
        % search in the finger list later the successor
        false -> 
            case closest_predecessor(State, NodeVector) of
                % no info available
                nil ->
                    {found_me, {State#state.node_name, State#state.node_vector}};

                % ask this closest pred to *other* node(need to pass nil)
                {RetNodeName, RetNodeVector} ->  
                    {not_found, {RetNodeName, RetNodeVector}}
            end
    end.

return_successor_info(find_successor_with_succlist, RetVal, NodeVector, From, State) ->
    case RetVal of
        {found_me, MyNode} -> 
            {reply, {MyNode, State#state.succlist}, State};
        {_, {InqNode, _InqVector}} ->
            gen_server:cast({global, InqNode}, 
                            {find_successor_ask_other, 
                             find_successor_with_succlist, NodeVector, From}),
            {noreply, State}
    end;
return_successor_info(find_successor, RetVal, NodeVector, From, State) ->
    case RetVal of
        {not_found, {InqNode, _InqVector}} ->
            gen_server:cast({global, InqNode}, 
                            {find_successor_ask_other, 
                             find_successor, NodeVector, From}),
            {noreply, State};
        {_, NewSucc} ->  % both found_other & found_me, in case only name is needed
            {reply, NewSucc, State}
    end.

%%--------------------------------------------------------------------
%% @doc it is trying to find the closest preceding node in local state.
%% 
%% (1) the successor of the target id is in this node's finger table range:
%%       in this case I want to forwad the query to the closest preceding node.
%%
%% (2) the successor of the target id is out of my finger table range:
%%       in this case, from the algorithm, the query is forwarded to 
%%       the node which is the farthest node from this node in the local
%%       finger table
%%
%% algorithm: refer to [1]:
%%
%% // search the local table for the highest predecesor of id
%% n.closest_preceding_node(id)
%%   for i = m downto 1
%%     if(finger[i] in (n, id))
%%       return finger[i]
%%   return n
%% @end
%%--------------------------------------------------------------------
closest_predecessor(State, NodeVector) -> 
    closest_predecessor_in(State#state.node_vector, 
                           NodeVector,
                           lists:reverse(State#state.finger)). 

closest_predecessor_in(_LocalVector, _NodeVector, []) -> nil;
closest_predecessor_in(LocalVector, NodeVector, FingerList) ->
    {FingerName, FingerVector} = hd(FingerList),
    case is_between(LocalVector, FingerVector, NodeVector) of
        true ->
            BareName = list_to_atom( atom_to_list(FingerName) -- ?PROCESS_PREFIX ),
            case hm_misc:is_alive(BareName) of
                true -> {FingerName, FingerVector};
                false ->
                    closest_predecessor_in(LocalVector, NodeVector, tl(FingerList))
            end;
        false ->
            closest_predecessor_in(LocalVector, NodeVector, tl(FingerList))
    end.

%%--------------------------------------------------------------------
%% @doc condition: (From, To) = {Target | From < Target < To}
%%  used by closest_preceding_node, notify algorithm
%% @end
%%--------------------------------------------------------------------
is_between(From, _Target, To) when From =:= To -> true;
is_between(From, Target, To) when From  <  To ->
    case (From < Target) and (Target < To) of
        true -> true;
        false -> false
    end;
is_between(From, Target, To) when From  >  To ->
    case ((From < Target) or (Target < To)) of
        true -> true;
        false -> false
    end.

%%--------------------------------------------------------------------
%% @doc condition: (From, To] = {Target | From < Target =< To}
%%  used by find_successor algorithm
%% @end
%%--------------------------------------------------------------------
is_between2(From, _Target, To) when From =:= To ->
    true;
is_between2(From, Target, To) when From  <  To ->
    case (From < Target) and (Target =< To) of
        true -> true;
        false -> false
    end;
is_between2(From, Target, To) when From  >  To ->
    case ((From < Target) or (Target =< To)) of
        true -> true;
        false -> false
    end.

find_successor_ask_other(Key, NodeVector, From, State) ->
    case find_successor_in(NodeVector, State) of
        %% what you are looking for is me, I'm returning my information to you
        {found_me, NewSucc} ->
            case Key =:= find_successor_with_succlist of
                true  -> gen_server:reply(From, {NewSucc, State#state.succlist});
                false -> gen_server:reply(From, NewSucc)
            end;

        %% what you are looking for is my successor, 
        %% if you only need a name, I'm giving it to you now,
        %% if you also need successor list of it, I'll forward you to it.
        {found_other, {InqNode, InqNodeVector}} ->
            case Key =:= find_successor_with_succlist of
                true  -> gen_server:cast({global, InqNode}, {return_with_succlist, From});
                false -> gen_server:reply(From, {InqNode, InqNodeVector})
            end;

        %% not found here, forward it to other again...
        {not_found, {InqNode, _InqVector}} ->
            gen_server:cast({global, InqNode}, {find_successor_ask_other, Key, NodeVector, From})
    end.

%%--------------------------------------------------------------------
%% @doc this routine is called periodically from hm_stabilizer
%%      to maintail predecessor 
%%
%% algorithm: refer to [1]:
%%
%% // n' thinks it might be my predecessor
%% n.notify(n')
%%   if(predecessor is nil or n' in (predecessor, n))
%%     predecessor = n'
%% 
%% @end
%%--------------------------------------------------------------------
notify(NodeInfo, State) -> 
    case hm_misc:check_pred_and_successor(State) of
        % predecessor == nil
        {succ_exists, pred_is_nil}     -> State#state{predecessor = NodeInfo};

        % predecessor == other node
        {succ_exists, pred_is_not_nil} -> 
            {_, PredVector} = State#state.predecessor,
            {_, NodeVector} = NodeInfo,
            case is_between(PredVector, NodeVector, State#state.node_vector) of
                true  -> State#state{predecessor = NodeInfo};
                false -> State
            end;

        % finger table lentgh == 0, possible bug
        {no_succ_exits, _}               -> State

    end.

%%--------------------------------------------------------------------
%% @doc this routine is called periodically from hm_stabilizer
%%      to maintain its successor by checking its current successor
%%      that if it is the immediate predecessor of it
%%
%% algorithm: refer to [1]:
%%
%% // callded periodically, veries n's immediate successor,
%% // and tells the successor about n
%% n.stabilize()
%%   x = successor.predecessor,
%%   if( x in (n, successor))
%%     successor = x
%%   successor.notify(n)
%% 
%% @end
%%--------------------------------------------------------------------
stabilize_in(nil, State) -> State;
stabilize_in({PredName, PredVector}, State) ->
    % check if new successor exists
    {_SuccName, SuccVector} = get_successor(State),
    case is_between(State#state.node_vector, PredVector, SuccVector) of 
        % update successor
        true  -> State#state{finger=[{PredName,PredVector}|
                                                tl(State#state.finger)]};
        % no need to update
        false -> State
    end.

get_successor(State) -> 
    case length(State#state.finger) > 0 of
        true  -> hd(State#state.finger);
        false -> nil % basically I don't see the case of successor is nil,
                     % if it is, I let it clash.
    end.

check_pred_in(State) when State#state.predecessor =:= nil ->
    State;
check_pred_in(State) ->
    % Check predecessor and set nil if failed
    {PredName, _PredVector} = State#state.predecessor,
    BareName = list_to_atom( atom_to_list(PredName) -- ?PROCESS_PREFIX ),
    case hm_misc:is_alive(BareName) of 
        true  -> State;
        false -> State#state{predecessor = nil}
    end.
