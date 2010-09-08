-module(hm_router).
-behaviour(gen_server).
-vsn('0.1').

-export([
        lookup/1, 
        name/1,
        start/1, 
        start_link/1,
        state_info/1, 
        state_info/2, 
        stop/0, 
        stop/1 
        ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").

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

terminate(Reason, State) ->
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    hm_misc:crypto_stop(),
    ok.

-spec(lookup(Key::atom()) -> atom()).
% returns in the form of harmonia_foo
lookup(Key) ->
    KeyVector = hm_misc:get_digest(Key),
    {ok, RegName} = hm_misc:get_rand_procname(),
    {SuccName, _} = gen_server:call(
                        {global, name(RegName)}, 
                        {find_successor,
                        KeyVector, nil}
                    ),
    SuccName.

state_info(RegName) ->
    gen_server:call({global, name(RegName)}, state_info).
state_info(RegName, NodeName) ->
    gen_server:call({name(RegName), NodeName}, state_info).

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

handle_cast(stop, State) -> {stop, normal, State};

handle_cast({find_successor_ask_other, NodeVector, From}, State) ->
    RetVal = find_successor_in(NodeVector, State),

    ?info_p("find_successor_ask_other: RetVal:[~p] NodeVector:[~p] From:[~p] ~n", 
            State#state.node_name, [RetVal, NodeVector, From]),
    case RetVal of
        {found, NewSucc} ->
            % reply to originator
            gen_server:reply(From, NewSucc);
        {not_found, {InqNode, _InqVector}} ->
            % forward message
            gen_server:cast({global, InqNode}, {find_successor_ask_other, NodeVector, From})
    end,
    {noreply, State};

handle_cast({fix_finger, Next, NewSuccessor, Finger}, State) ->
    NewFinger = hm_misc:replace_nth(Next, NewSuccessor, Finger),
    {noreply, State#state{finger = NewFinger}};

handle_cast({set_succlist, SuccList}, State) ->
    {noreply, State#state{succlist = SuccList}};

handle_cast({notify, NodeInfo}, State) ->
    case hm_misc:check_pred_and_successor(State) of
        {succ_exists, true, _}  -> 
            NewState = State#state{predecessor = NodeInfo};

        {succ_exists, false, pred_is_nil} -> 
            NewState = State#state{predecessor = NodeInfo};

        {succ_exists, false, pred_is_not_nil} -> 
            {_, PredVector} = State#state.predecessor,
            {_, NodeVector} = NodeInfo,

            case hm_misc:is_between(PredVector, 
                                     NodeVector, 
                                     State#state.node_vector) of
                true ->
                    NewState = State#state{predecessor = NodeInfo};
                false -> 
                    NewState = State
            end;
        {no_succ_exits, false, _} ->
            NewState = State
    end,
    {noreply, NewState};

handle_cast({stabilize, nil}, State) ->
    % if successor has no predecessor, skip
    ?info_p("stabilize:called:[~p].~n", State#state.node_name, [State]),
    {noreply, State};
handle_cast({stabilize, {PredName, PredVector} = _PredOfSucc}, State) ->
    ?info_p("stabilize:called:[~p].~n", State#state.node_name, [State]),
    % check if new successor exists
    {_SuccName, SuccVector} = hm_misc:get_successor(State),
    case hm_misc:is_between(State#state.node_vector, 
                            PredVector, 
                            SuccVector) of 
        % update successor
        true  -> NewState = State#state{finger=[{PredName,PredVector}|
                    tl(State#state.finger)]};
        % no need to update
        false -> NewState = State
    end,
    {noreply, NewState};

handle_cast(check_pred, State) when State#state.predecessor =:= nil ->
    {noreply, State};
handle_cast(check_pred, State) ->
    % Check predecessor and set nil if failed
    {PredName, _PredVector} = State#state.predecessor,
    NewState = 
        case hm_misc:is_alive(PredName) of 
            true -> State;
            false -> State#state{predecessor = nil}
        end,
    {noreply, NewState}.


handle_call({find_successor, NodeVector, CurrentFix}, From, State) ->
    find_successor_handle_call_in(
        find_successor, 
        NodeVector, CurrentFix, From, State
    );

handle_call({find_successor_with_succlist, NodeVector, CurrentFix}, From, State) ->
    find_successor_handle_call_in(
        find_successor_with_succlist,
        NodeVector, CurrentFix, From, State
    );

handle_call(copy_succlist, _From, State) ->
    ?info_p("copy_succlist:succlist:[~p].~n", State#state.node_name, [State#state.succlist]),
    {reply, State#state.succlist, State};

handle_call(get_predecessor, _From, State) ->
    {reply, State#state.predecessor, State};

handle_call(get_successor, _From, State) ->
    Ret = hm_misc:get_successor_alive(State), 
    {reply, Ret, State};

handle_call(state_info, _From, State) ->
    {reply, {ok, State}, State}.


name(Name) -> list_to_atom("hm_router_" ++ atom_to_list(Name)).

find_successor_handle_call_in(Handlecall_Key, NodeVector, CurrentFix, From, State) ->
    % when inquired by other, 'CurrentFix' should not be modified
    NewState = 
        case CurrentFix of
            nil -> State; % inquired by other, no change to current_fix
            _   -> State#state{current_fix = CurrentFix} % inquired by self-stabilizer
        end,
    case NodeVector =:= State#state.node_vector of 
        % when node vector == current node, return current node
        true ->
            {reply, {State#state.node_name, State#state.node_vector}, NewState};
        false ->
            RetVal = find_successor_in(NodeVector, State),
            ?info_p("~p: RetVal:[~p] NodeVector:[~p]. CurrentFix:[~p] From:[~p] ~n", 
                    State#state.node_name, [Handlecall_Key, RetVal, NodeVector, CurrentFix, From]),
            return_successor_info(Handlecall_Key, RetVal, NodeVector, From, NewState)
    end.

find_successor_in(NodeVector, State) ->
    {Succ, SuccVector} = hd(State#state.finger),
    case hm_misc:is_between2(State#state.node_vector, 
                             NodeVector,
                             SuccVector) of
        % the successor of NodeVector is this node
        true  -> 
            ?info_p("find_successor_in: Succ:[~p].~n", State#state.node_name, [Succ]),
            {found, {Succ, SuccVector}};

        % my successor is not the successor of NodeVector,
        % search in the finger list later the successor
        false -> 
            case ask_closest_predecessor(State, NodeVector) of
                {exists_in_local, TargetNode} -> 
                    % I'm successor of NodeVector
                    ?info_p("find_successor_in: TargetNode:[~p].~n", State#state.node_name, [TargetNode]),
                    {found, TargetNode};
                {not_exists_in_local, InqNode} -> 
                    ?info_p("find_successor_in: InqNode:[~p].~n", State#state.node_name, [InqNode]),
                    {not_found, InqNode}
            end
    end.

ask_closest_predecessor(State, NodeVector) -> 
    case hm_misc:closest_predecessor(State, NodeVector) of
        % no info available
        nil -> 
            ?info_p("ask_closest_pred: NodeVector(returning my name):[~p].~n",
                State#state.node_name, [NodeVector]),
            
            NewSucc = {exists_in_local, 
                {State#state.node_name, State#state.node_vector}};

        % ask this closest pred to *other* node(need to pass nil)
        {RetNodeName, RetNodeVector} ->  
            ?info_p("ask_closest_pred: RetNodeName:[~p] RetNodeVector:[~p] NodeVector:[~p].~n",
                State#state.node_name, [RetNodeName, RetNodeVector, NodeVector]),
            NewSucc = {not_exists_in_local, {RetNodeName, RetNodeVector}}
    end,
    NewSucc.


return_successor_info(Key, RetVal, NodeVector, From, NewState) ->
    case RetVal of
        {found, NewSucc} -> 
            return_successor_info_in(Key, NewSucc, NewState);
        {not_found, {InqNode, _InqVector}} ->
            % ask to InqNode about NodeVector
            gen_server:cast({global, InqNode}, {find_successor_ask_other, NodeVector, From}),
            {noreply, NewState}
    end.

return_successor_info_in(find_successor, NewSucc, NewState) ->
    {reply, NewSucc, NewState};
return_successor_info_in(find_successor_with_succlist, NewSucc, NewState) ->
    {reply, {NewSucc, NewState#state.succlist}, NewState}.
