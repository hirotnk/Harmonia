-module(hm_stabilizer).
-behaviour(gen_fsm).
-vsn('0.1').

-export([
        fixfinger_loop/2, 
        name/1,
        stabilize_loop/2, 
        start_link/1, 
        stop/1 
        ]).
-export([init/1, terminate/3]).

-include("harmonia.hrl").

start_link(RegName) ->
    gen_fsm:start_link({global, name(RegName)}, ?MODULE, RegName, []).

stop(RegName) ->
    ?info_p("stop:stopping:[~p].~n", RegName, [RegName]),
    gen_fsm:send_event({global, name(RegName)}, stop).

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
    gen_server:cast({global, hm_router:name(RegName)}, check_pred),

    % node is checked to be alive inside get_successor
    {ok, State} = gen_server:call({global, hm_router:name(RegName)}, state_info),
    Succ = hm_misc:get_successor_alive(State),
    ?info_p("stabilize_loop:SuccName:[~p].~n", RegName, [Succ]),

    {NodeName, _NodeVector} = 
        case Succ of
            % currently no successor
            {error, no_successor} -> 
                {State#state.node_name, State#state.node_vector};
            
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
                {SuccName, SuccVector}
        end,
    % get predecessor of successor, and check if new successor exists
    check_new_successor(NodeName, RegName),

    % notify to "successor" about me
    gen_server:cast({global, NodeName}, {notify, {State#state.node_name, State#state.node_vector}}),

    {next_state, fixfinger_loop, RegName, ?stabilize_interval}.


check_new_successor(SuccName, RegName) ->
    PredOfSucc = gen_server:call({global, SuccName}, get_predecessor),
    gen_server:cast({global, hm_router:name(RegName)}, {stabilize, PredOfSucc}).


make_succ_list({SuccName, SuccVector}, MyNodeName) ->
    SuccList = [{SuccName, SuccVector} |
                gen_server:call({global, SuccName}, copy_succlist)],
    NewSuccList = 
        case length(SuccList) > ?succ_list_len of
            true -> lists:sublist(SuccList, 1, ?succ_list_len);
            false -> SuccList
        end,
    ?info_p("stabilize_loop:Updated SuccList:[~p].~n", MyNodeName, [NewSuccList]),
    gen_server:cast({global, MyNodeName}, {set_succlist, NewSuccList}).


fixfinger_loop(stop, RegName) ->
    {stop, normal, RegName};

fixfinger_loop(timeout, RegName) ->
    ?info_p("fixfinger_loop:[~p].~n", RegName, [RegName]),

    {ok, State} = gen_server:call({global, hm_router:name(RegName)}, state_info),

    Current = 
        case (State#state.current_fix + 1) > ?max_finger of
            true  -> 1;
            false -> State#state.current_fix + 1
        end,
    NodeVector = (State#state.node_vector + int2pow(Current - 1)) rem ?max_key_value,

    % 1. Check if myself < 'NodeVector' <= my successor
    % 2. If not 1, then check closest preceding node and forward 
    %    inquiry to it
    NewSucc = gen_server:call({global, hm_router:name(RegName)}, {find_successor, NodeVector, Current}),

    % this only does replace my finger's Current-th entry
    ?info_p("fixfinger_loop:Current:[~p], NewSucc:[~p], NodeVector:[~p], State#state.finger:[~p].~n", 
                RegName, [Current, NewSucc, NodeVector, State#state.finger]),
    gen_server:cast({global, hm_router:name(RegName)}, {fix_finger, Current, NewSucc, State#state.finger}),
    {next_state, stabilize_loop, RegName, ?fixfinger_interval}.

int2pow(0) -> 1;
int2pow(Cur) -> int2pow_in(Cur, 1).
int2pow_in(0, Acc)-> Acc;
int2pow_in(Cur, Acc)-> int2pow_in(Cur-1, Acc * 2).



init(RegName) -> 
    {ok, stabilize_loop, RegName, ?stabilize_interval}.

name(Name) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Name)).

