-module(hm_misc).
-compile([export_all]).

-include("harmonia.hrl").


get_successor(State) -> 
    case length(State#state.finger) > 0 of
        true  -> hd(State#state.finger);
        false -> nil
    end.

get_successor_alive(State) when length(State#state.finger) > 0 ->
    {SuccName, SuccVector} = hd(State#state.finger),
    % check if the Succ is alive 
    case is_alive(SuccName) of
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

closest_predecessor(State, NodeVector) -> 
    closest_predecessor_in(State#state.node_vector, 
                           NodeVector,
                           lists:reverse(State#state.finger),
                           State#state.predecessor). 

closest_predecessor_in(_LocalVector, _NodeVector, [], _Pred) -> nil;
closest_predecessor_in(LocalVector, NodeVector, FingerList, nil) ->
    {FingerName, FingerVector} = hd(FingerList),
    case is_between(LocalVector, FingerVector, NodeVector) of
        true -> {FingerName, FingerVector};
        false -> closest_predecessor_in(LocalVector, NodeVector, tl(FingerList), nil)
    end;
closest_predecessor_in(LocalVector, NodeVector, FingerList, Pred) ->
    {FingerName, FingerVector} = hd(FingerList),
    case is_between(LocalVector, FingerVector, NodeVector) of
        true -> {FingerName, FingerVector};

        % seems to need to include predecessor to be judged
        % otherwise, finger table won't get correct?
        false -> 
            {PredName, PredVector} = Pred,
            case is_between(LocalVector, PredVector, NodeVector) of
                 true -> {PredName, PredVector};
                 false -> 
                    closest_predecessor_in(LocalVector, NodeVector, 
                                           FingerList, nil)
            end
    end.

%% condition: From < Target < To
%% used to find predecessor
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

%% used for find_successor, 
%% condition: From < Target =< To
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

get_rand_procname() ->
    % TODO: these name shoud be taken from configuration file
    Pre1 = "harmonia_", 
    Pre2 = "harmonia_ds", 
    Pre3 = "harmonia_table", 
    Pre4 = "harmonia_stabilizer", 
    ProcList = [X || X <- registered(), 
                lists:prefix(Pre1,atom_to_list(X)) andalso not 
                lists:prefix(Pre2,atom_to_list(X)) andalso not
                lists:prefix(Pre3,atom_to_list(X)) andalso not
                lists:prefix(Pre4,atom_to_list(X)) ],
    case length(ProcList) of 
        0 -> {error, instance};
        N -> {ok, lists:nth(random:uniform(N), ProcList)}
    end.

get_first_alive_entry([]) -> {error, none};
get_first_alive_entry([{Name, _Vector} = FirstNode|NodeList]) ->
    case is_alive(Name) of
        true  -> FirstNode;
        false ->
            get_first_alive_entry(NodeList)
    end.

is_alive(NodeName) ->
    case whereis(NodeName) of
        undefined -> false;
        _         -> true
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
   


diff(L1,L2) -> diff_in(L1,L2,[]).
diff_in([],[],L3) -> L3;
diff_in([],[H2|L2],L3) -> 
    diff_in([],L2,L3++[H2]);
diff_in([H1|L1],[],L3) ->
    diff_in(L1,[],L3++[H1]);
diff_in([H1|L1],[H2|L2],L3) ->
    case (H1 =/= H2) of
        true -> diff_in(L1, L2, L3++[H1]);
        false -> diff_in(L1, L2, L3)
    end.

check_pred_and_successor(State) ->
    Name = State#state.node_name,

    % includes info about predecessor also
    case State#state.predecessor =:= nil of 
        true ->
            Pred = pred_is_nil;
        false ->
            Pred = pred_is_not_nil
    end,
    case length(State#state.finger) > 0 of
        true ->
            {SuccName, _} = hd(State#state.finger),
            case Name =:= SuccName of 
                true -> {succ_exists, true, Pred};
                false -> {succ_exists, false, Pred}
            end;
        false ->
            {no_succ_exists, false, Pred}
    end.

is_pred_nil(State) ->
    case State#state.predecessor =:= nil of
        true -> true;
        false -> false
    end.

make_request_list(TargetName, SuccListTarget) ->
    SuccList = 
        case length(SuccListTarget) > 0 of
            true ->
                SuccTemp = sets:to_list(sets:from_list(SuccListTarget)),
                [{TargetName, instance} | lists:filter(fun({X,_})-> X =/= TargetName end, SuccTemp)];
            false ->
                [{TargetName, instance}]
        end.

make_request_list_from_dt(DomainName, TableName) ->
    % get atom() of node name
    %% TODO: this two line should use find_successor_with_succlist ? for performance
    NodeName = harmonia:lookup(DTName=list_to_atom(DomainName ++ TableName)),
    SuccList = gen_server:call(NodeName, copy_succlist),

    NodeList = make_request_list(NodeName, SuccList).


get_digest_from_atom(Key) ->
    <<Vector:160>> = crypto:sha(atom_to_list(Key)),
    Vector rem ?max_key_value.

get_digest_from_list(Key) ->
    <<Vector:160>> = crypto:sha(Key),
    Vector rem ?max_key_value.

search_table_attlist(DTName, TblList) ->
    case lists:keyfind(DTName, 2, TblList) of
        false -> {error, no_table};
        {Tid, DTName, AttList} -> {ok, Tid, AttList}
    end.
