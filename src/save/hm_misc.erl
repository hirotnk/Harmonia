-module(hm_misc).
-compile([export_all]).

-include("harmonia.hrl").


get_successor(State) -> 
    case length(State#state.finger) > 0 of
        true  -> hd(State#state.finger);
        false -> nil
    end.

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
                           State#state.predecessor,
                           State#state.succlist). 

closest_predecessor_in(_LocalVector, _NodeVector, [], _Pred, _SuccList) -> 
    nil;
closest_predecessor_in(LocalVector, NodeVector, FingerList, Pred, SuccList) ->
    {FingerName, FingerVector} = hd(FingerList),

    % need to evaluate successor list too
    {SuccName, SuccVector} = 
        case (SuccList =:= []) of
            true -> {nil,nil};
            false -> hd(SuccList)
        end,

    %
    % TODO: this code is awfully messy, need to clean up ASAP
    %
    case Pred of 
        nil ->
            case is_between(LocalVector, FingerVector, NodeVector) of
                true -> 
                    case is_alive(FingerName) of
                        true -> {FingerName, FingerVector};
                        false -> 
                            case ((SuccName =/= nil) and is_alive(SuccName)) of 
                                true -> {SuccName, SuccVector};
                                false ->
                                    closest_predecessor_in(LocalVector, 
                                                           NodeVector, 
                                                           tl(FingerList), 
                                                           Pred, tl(SuccList))
                            end
                    end;
                false -> 
                    case ((SuccName =/= nil) and is_alive(SuccName)) of 
                        true -> {SuccName, SuccVector};
                        false ->
                            closest_predecessor_in(LocalVector, 
                                                   NodeVector, 
                                                   tl(FingerList), 
                                                   Pred, tl(SuccList))
                    end
            end;

        {PredName, PredVector} -> 
            case is_between(LocalVector, FingerVector, NodeVector) of
                true -> case is_alive(FingerName) of
                            true -> {FingerName, FingerVector};
                            false ->
                                case ((SuccName =/= nil) and is_alive(SuccName)) of 
                                    true -> {SuccName, SuccVector};
                                    false ->
                                        closest_predecessor_in(LocalVector, 
                                                               NodeVector, 
                                                               tl(FingerList), 
                                                               Pred, tl(SuccList))
                                end
                        end;

                %% seems to need to include predecessor to be judged
                %% otherwise, finger table won't get correct?
                false -> 
                    case is_between(LocalVector, PredVector, NodeVector) of
                         true -> case is_alive(PredName) of
                                    true -> {PredName, PredVector};
                                    false ->
                                        case ((SuccName =/= nil) and is_alive(SuccName)) of 
                                            true -> {SuccName, SuccVector};
                                            false ->
                                                closest_predecessor_in(LocalVector, 
                                                                       NodeVector, 
                                                                       tl(FingerList), 
                                                                       Pred, tl(SuccList))
                                        end
                                 end;

                         false -> 
                                case ((SuccName =/= nil) and is_alive(SuccName)) of 
                                    true -> {SuccName, SuccVector};
                                    false ->
                                        closest_predecessor_in(LocalVector, 
                                                               NodeVector, 
                                                               tl(FingerList), 
                                                               Pred, tl(SuccList))
                                end
                     end
            end
    end.

%% condition: From < Target < To
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
    Pre3 = "harmonia_stabilizer", 
    ProcList = [X || X <- registered(), 
                lists:prefix(Pre1,atom_to_list(X)) andalso not 
                lists:prefix(Pre2,atom_to_list(X)) andalso not
                lists:prefix(Pre3,atom_to_list(X)) ],
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

