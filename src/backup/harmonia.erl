-module(harmonia).
-behaviour(gen_server).
-vsn('0.1').

-export([start/1, stop/0, stop/1, 
         lookup/1, state_info/1, state_info/2, name/1]).
-export([start_link/1]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").



start([]) -> ok;
start(NodeNameList) ->
    start_link(hd(NodeNameList)),
    start(tl(NodeNameList)).

start_link({create, RegName}) ->
    gen_server:start_link({local, 
                           name(RegName)}, 
                           ?MODULE, 
                           {create, name(RegName)}, []);
start_link({join, RegName, RootName}) ->
    gen_server:start_link({local, 
                           name(RegName)}, 
                           ?MODULE, 
                           {{join, RootName}, name(RegName)}, []).

stop() ->
    gen_server:cast(?MODULE, stop).

stop(RegName) ->
    gen_server:cast(name(RegName), stop).

terminate(_Reason, _State) ->
    hm_misc:crypto_stop(),
    ok.

lookup(Key) ->
    KeyVector = get_digest(Key),
    {ok, RegName} = hm_misc:get_rand_procname(),
    {SuccName, _} = gen_server:call(RegName, {find_successor, KeyVector, nil}),
    SuccName.

state_info(RegName) ->
    gen_server:call(name(RegName), state_info).
state_info(RegName, NodeName) ->
    gen_server:call({name(RegName), NodeName}, state_info).

init({Op, NodeName}) ->
    hm_misc:crypto_start(),
    NodeVector = get_digest(NodeName),
    State = #state{node_name = NodeName, node_vector = NodeVector},
    case Op of 
        create -> 
            NewState = State#state{finger = [{NodeName, NodeVector}]};

        {join, RootNodeName} ->
            % when find_successor is called from other node,
            % current fix should not modified, so passing nil
            NewSucc = gen_server:call(name(RootNodeName), 
                                      {find_successor, NodeVector, nil}),
            NewState = State#state{finger = [NewSucc]}
    end,
    {ok, NewState}.

handle_cast(stop, State) -> {stop, normal, State};

handle_cast({find_successor_ask_other, NodeVector, From}, State) ->
    RetVal = find_successor_in(NodeVector, State),

    ?debug_p("find_successor_ask_other: RetVal:[~p] NodeVector:[~p] From:[~p] ~n", 
            State#state.node_name, [RetVal, NodeVector, From]),
    case RetVal of
        {found, NewSucc} ->
            % reply to originator
            gen_server:reply(From, NewSucc);
        {not_found, {InqNode, _InqVector}} ->
            % forward message
            gen_server:cast(InqNode, {find_successor_ask_other, NodeVector, From})
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
    ?debug_p("stabilize:called:[~p].~n", State#state.node_name, [State]),
    {noreply, State};
handle_cast({stabilize, {PredName, PredVector} = _PredOfSucc}, State) ->
    ?debug_p("stabilize:called:[~p].~n", State#state.node_name, [State]),
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

    % when inquired by other, 'CurrentFix' should not be modified
    case CurrentFix of
        nil -> % inquired by other
            NewState = State;
        _   -> % inquired by self-stabilizer
            NewState = State#state{current_fix = CurrentFix}
    end,

    RetVal = find_successor_in(NodeVector, State),
    ?debug_p("find_successor: RetVal:[~p] NodeVector:[~p]. CurrentFix:[~p] From:[~p] ~n", 
            State#state.node_name, [RetVal, NodeVector, CurrentFix, From]),

    case RetVal of
        {found, NewSucc}                   -> {reply, NewSucc, NewState};
        {not_found, {InqNode, _InqVector}} ->
            % ask to InqNode about NodeVector
            gen_server:cast(InqNode, {find_successor_ask_other, NodeVector, From}),
            {noreply, NewState}
    end;

handle_call(build_succlist, _From, State) ->
    SuccList = [{State#state.node_name, State#state.node_vector} | 
                lists:sublist(State#state.finger, 1, ?succ_list_len - 1)],
    {reply, SuccList, State};

handle_call(copy_succlist, _From, State) ->
    ?debug_p("copy_succlist:succlist:[~p].~n", State#state.node_name, [State#state.succlist]),
    {reply, State#state.succlist, State};

handle_call(get_predecessor, _From, State) ->
    {reply, State#state.predecessor, State};

handle_call(get_successor, _From, State) ->
    Ret = hm_misc:get_successor_alive(State), 
    {reply, Ret, State};

handle_call(state_info, _From, State) ->
    {reply, {ok, State}, State}.


name(Name) -> list_to_atom("harmonia_" ++ atom_to_list(Name)).

find_successor_in(NodeVector, State) ->
    {Succ, SuccVector} = hd(State#state.finger),
    case hm_misc:is_between2(State#state.node_vector, 
                             NodeVector,
                             SuccVector) of
        % the successor of NodeVector is this node
        true  -> 
            ?debug_p("find_successor_in: Succ:[~p].~n", State#state.node_name, [Succ]),
            {found, {Succ, SuccVector}};

        % my successor is not the successor of NodeVector,
        % search in the finger list later the successor
        false -> 
            case ask_closest_predecessor(State, NodeVector) of
                {exists_in_local, TargetNode} -> 
                    % I'm successor of NodeVector
                    ?debug_p("find_successor_in: TargetNode:[~p].~n", State#state.node_name, [TargetNode]),
                    {found, TargetNode};
                {not_exists_in_local, InqNode} -> 
                    ?debug_p("find_successor_in: InqNode:[~p].~n", State#state.node_name, [InqNode]),
                    {not_found, InqNode}
            end
    end.

ask_closest_predecessor(State, NodeVector) -> 
    case hm_misc:closest_predecessor(State, NodeVector) of
        % no info available
        nil -> 
            ?debug_p("ask_closest_pred: NodeVector(returning my name):[~p].~n",
                State#state.node_name, [NodeVector]),
            
            NewSucc = {exists_in_local, 
                {State#state.node_name, State#state.node_vector}};

        % ask this closest pred to *other* node(need to pass nil)
        {RetNodeName, RetNodeVector} ->  
            ?debug_p("ask_closest_pred: RetNodeName:[~p] RetNodeVector:[~p] NodeVector:[~p].~n",
                State#state.node_name, [RetNodeName, RetNodeVector, NodeVector]),
            NewSucc = {not_exists_in_local, {RetNodeName, RetNodeVector}}
    end,
    NewSucc.

get_digest(Key) ->
    <<Vector:160>> = crypto:sha(atom_to_list(Key)),
    Vector rem ?max_key_value.

