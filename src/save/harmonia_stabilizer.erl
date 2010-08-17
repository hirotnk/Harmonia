-module(harmonia_stabilizer).
-behaviour(gen_fsm).
-vsn('0.1').

-export([start_link/1, stop/1, stabilize_loop/2, fixfinger_loop/2, name/1]).
-export([init/1, terminate/3]).

-include("harmonia.hrl").



start_link(RegName) ->
    gen_fsm:start_link({local, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    ?debug_print("stop:stopping:[~p].~n", RegName, [RegName]),
    gen_fsm:send_event(name(RegName), stop).

terminate(_Reason, _StateName, _State) -> ok.

stabilize_loop(stop, RegName) ->
    {stop, normal, RegName};

%
% 1. x = get pred of successor
% 2. check if x is my immediage successor
% 3. notify about me to successor
%
stabilize_loop(timeout, RegName) ->

    % check predecessor - if it's dead, set nil to my pred
    % TODO: this might be independent from stabilizer
    gen_server:cast(harmonia:name(RegName), check_pred),
    {ok, State} = gen_server:call(harmonia:name(RegName), state_info),

    % node is checked to be alive inside get_successor
    Succ = gen_server:call(harmonia:name(RegName), get_successor),

    %
    %
    % TODO: code here is messy. need to clean up ASAP!!
    %
    %
    ?debug_print("stabilize_loop:SuccName:[~p].~n", RegName, [Succ]),
    {NodeName, NodeVector} = 
        case Succ of
            {error, no_successor} -> 
                {State#state.node_name, State#state.node_vector};

            {error, successor_dead} -> 
                case harmonia_misc:get_first_alive_entry(State#state.finger) of
                    {error, none} -> 
                        ?debug_print("stabilize_loop:{error, none}.~n", RegName, []),
                        {State#state.node_name, State#state.node_vector};

                    % if successor is dead and there is some alive successor,
                    % build a new successor list
                    {NewSucc, NewSuccVector} -> 
                        ?debug_print("stabilize_loop:NewSucc:[~p] NewSuccVector:[~p].~n", RegName, 
                               [NewSucc, NewSuccVector]),
                        SuccList = [{NewSucc, NewSuccVector} |
                                    gen_server:call(NewSucc, copy_succlist)],
                        case length(SuccList) > ?succ_list_len of
                            true -> NewSuccList = lists:sublist(SuccList, 1, ?succ_list_len);
                            false -> NewSuccList = SuccList
                        end,
                        gen_server:cast(State#state.node_name,
                                        {set_succlist, NewSuccList}),

                        ?debug_print("stabilize_loop:Updated SuccList:[~p].~n", RegName, [NewSuccList]),

                        {NewSucc, NewSuccVector}
                end;
            {ok, {SuccName, SuccVector}} ->
                SuccListOfSucc = gen_server:call(SuccName, copy_succlist),
                case (length(SuccListOfSucc) < ?succ_list_len) of
                    true -> 
                        % build a new succssor list
                        SuccList = gen_server:call(SuccName, build_succlist);
                    false ->
                        % update succssor list
                        SuccList = [{SuccName, SuccVector} |
                            lists:sublist(SuccListOfSucc, 1, ?succ_list_len)]
                end,
                gen_server:cast(State#state.node_name, 
                                {set_succlist, SuccList}),
                ?debug_print("stabilize_loop:New SuccList:[~p].~n", RegName, [SuccList]),

                {SuccName, SuccVector}
        end,

    % get predecessor of successor, and check if new successor exists
    PredOfSucc = gen_server:call(NodeName, get_predecessor),
    gen_server:cast(harmonia:name(RegName), {stabilize, PredOfSucc}),

    % notify to "successor" about me
    ?debug_print("stabilize_loop:[~p] is notifying to :[~p].~n", RegName, [RegName, NodeName]),
    gen_server:cast(NodeName, {notify, {State#state.node_name, State#state.node_vector}}),
    {next_state, fixfinger_loop, RegName, ?stabilize_interval}.


fixfinger_loop(stop, RegName) ->
    {stop, normal, RegName};

fixfinger_loop(timeout, RegName) ->
    Ref = make_ref(),
    ?debug_print("Ref:[~p], fixfinger_loop:[~p].~n", RegName, [Ref, RegName]),

    {ok, State} = gen_server:call(harmonia:name(RegName), state_info),
    case (State#state.current_fix + 1) > ?max_finger of
        true  -> Next = 1;
        false -> Next = State#state.current_fix + 1
    end,
    ?debug_print("Ref:[~p], node_vector:[~p], Next:[~p], max_key:[~p].~n", RegName, [Ref, State#state.node_vector, Next, ?max_key_value ]),
    TempNext = round(math:pow(2, Next - 1)),
    TempNext2 = (State#state.node_vector + TempNext),
    NodeVector = TempNext2 rem ?max_key_value,
    ?debug_print("Ref:[~p], TempNext:[~p], node_vector:[~p], ?max_key:[~p] temp_next:[~p] NodeVector:[~p]~n", RegName, [Ref, TempNext,State#state.node_vector,?max_key_value, TempNext2,NodeVector]),

    % this find_successor should return "alive" node
    NewSucc = gen_server:call(harmonia:name(RegName), {find_successor, NodeVector, Next}),

    % this only does replace my finger's Next-th entry
    ?debug_print("Ref:[~p], fixfinger_loop:Next:[~p], NewSucc:[~p], NodeVector:[~w], NodeVector:[~p], State#state.finger:[~p].~n", 
                 RegName, 
                [Ref, Next, NewSucc, NodeVector, NodeVector, State#state.finger]),
    gen_server:cast(harmonia:name(RegName), {fix_finger, Next, NewSucc, State#state.finger}),

    {next_state, stabilize_loop, RegName, ?fixfinger_interval}.



init(RegName) -> 
    {ok, stabilize_loop, RegName, ?stabilize_interval}.

name(Name) -> list_to_atom("harmonia_stabilizer_" ++ atom_to_list(Name)).

