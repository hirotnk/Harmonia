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

-module(hm_sim).
-behaviour(gen_server).
-vsn('0.1').

-export([
        start_link/0, 
        test_all/0,
        test_all/3,
        test_all/5,
        stop/0,
        add_data/1,
        add_data/2,
        register_node/1,
        make_node_list/2,
        show_stat/0,
        show_stat_rep/1
        ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").


start_link() ->
    hm_misc:crypto_start(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    hm_misc:crypto_stop(),
    gen_server:cast(?MODULE, stop).

terminate(Reason, State) ->
    io:format("terminate:Reason:[~p] State:[~p]~n", [Reason, State]),
    ok.

init(RegName) ->
    {ok, []}.

test_all() ->
    start_link(),
    register_node_list([foo,bar,hoge,cat,dog]),
    add_data(1,100).

test_all(NumNodeList, NumData, ReplicaNum) ->
    start_link(),
    register_node_list(make_node_list(NumNodeList, [])),
    add_data(1,NumData),
    show_stat(),
    show_stat_rep(ReplicaNum),
    destroy_data().

test_all(StartNodeNum, Diff, EndNodeNum, NumData, ReplicaNum) when StartNodeNum > EndNodeNum -> ok;
test_all(StartNodeNum, Diff, EndNodeNum, NumData, ReplicaNum) ->
    test_all(StartNodeNum, NumData, ReplicaNum),
    test_all(StartNodeNum + Diff, Diff, EndNodeNum, NumData, ReplicaNum).

make_node_list(0, NodeList) -> NodeList;
make_node_list(Num, NodeList) ->
    make_node_list(Num - 1, [list_to_atom("test" ++ integer_to_list(Num))|NodeList]).

register_node_list([]) -> ok;
register_node_list(Nlist) ->
    register_node(hd(Nlist)),
    register_node_list(tl(Nlist)).

register_node(NodeName) ->
    Nodevec = hm_misc:get_digest(NodeName),
    {ok, Nodevec} = gen_server:call(?MODULE, {register_node, {NodeName, Nodevec}}).

add_data(Key) ->
    Keyvec = hm_misc:get_digest(Key),
    {ok, Keyvec} = gen_server:call(?MODULE, {add_data, {Key, Keyvec}}).

add_data(From, To) when From > To -> ok;
add_data(From, To) ->
    add_data(From),
    add_data(From + 1, To).

show_stat() ->
    gen_server:call(?MODULE, showstat).

show_stat_rep(N) ->
    gen_server:call(?MODULE, {showstatrep, N}).

destroy_data() ->
    gen_server:call(?MODULE, destroydata).

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call(destroydata, _From, NodeList) ->
    {reply, ok, []};

handle_call(showstat, _From, NodeList) ->
    showstat(NodeList),
    {reply, ok, NodeList};

handle_call({showstatrep, N}, _From, NodeList) ->
    showstat_rep(NodeList, N),
    {reply, ok, NodeList};

handle_call({register_node, {NodeName, Nodevec}}, _From, NodeList) ->
    NewState = lists:sort([{Nodevec, NodeName, []}|NodeList]),
    {reply, {ok, Nodevec}, NewState};

handle_call({add_data, {Key, Keyvec}}, _From, NodeList) ->
    NewState = inskey({Key, Keyvec}, NodeList),
    {reply, {ok, Keyvec}, lists:sort(NewState)}.

inskey({Key, Keyvec}, [{FNodevec, FNodeName, FKeylist} | NodeList]=State) ->
    {Nodevec, NodeName, Keylist} = lists:last(NodeList),
    case Keyvec > Nodevec of
        true -> 
            [{FNodevec, FNodeName, [{Key,Keyvec} | FKeylist] } | NodeList];
        false -> 
            inskey_in({Key, Keyvec}, State, [])
    end.

inskey_in({Key, Keyvec}, [], NewList) -> lists:reverse(NewList);
inskey_in({Key, Keyvec}, [{Nodevec, NodeName, Keylist}|NodeList], NewList) ->
    case (Keyvec =< Nodevec) of 
        true -> 
            lists:append([lists:reverse(NodeList), [{Nodevec, NodeName, [{Key, Keyvec} | Keylist]}|NewList]]);
        false ->
            inskey_in({Key, Keyvec}, NodeList, [{Nodevec, NodeName, Keylist}|NewList])
    end.

showstat_rep(NodeListOrg, N) ->
    NodeList = reduce_nodelist(NodeListOrg, 0, N, []),
    showstat(NodeList).

reduce_nodelist([], Cur, N, NewList) -> NewList;
reduce_nodelist(NodeList, Cur, N, NewList) when Cur =:= N ->
    reduce_nodelist(NodeList, 0, N, NewList);
reduce_nodelist([{Nodevec, Nodename, Keylist}|NodeList], 0, N, NewList) ->
    reduce_nodelist(NodeList, 1, N, [{Nodevec, Nodename, Keylist}| NewList]);
reduce_nodelist([{_, _, Keylist}|NodeList], Cur, N, NewList) ->
    {Nodevec, Nodename, KeylistOld} = hd(NewList),
    reduce_nodelist(NodeList, Cur+1, N, [{Nodevec, Nodename, Keylist ++ KeylistOld}] ++ tl(NewList)).


showstat(NodeList) ->
    showstat_in(NodeList),
    {Avg, Sum} = calcavgsum(NodeList,[]),
    N = length(NodeList),
    SD = math:sqrt((Sum - (N*Avg*Avg))/(N - 1)),
    io:format("sd for ~5.5p nodes:[~10.5f] avg:[~10.5f], sd/avg:[~10.5f%] \n", [N, SD, Avg, abs(SD / Avg)*100]).

showstat_in([]) -> ok;
showstat_in([{Nodevec, NodeName, Keylist}|NodeList]) ->
    %io:format("Node:[~p] NodeVec:[~p] # of Data:[~p] \nData:[~w] \n\n", [NodeName, Nodevec, length(Keylist), Keylist]),
    %io:format("Node:[~p] NodeVec:[~p] # of Data:[~p]\n", [NodeName, Nodevec, length(Keylist)]),
    showstat_in(NodeList).

calcavgsum([], L) -> 
    Avg = lists:sum(L)/length(L),
    Sum = lists:sum(lists:map(fun(X) -> X*X end, L)),
    {Avg, Sum};
calcavgsum([{Nodevec, NodeName, Keylist}|NodeList], L) ->
    NumData = length(Keylist),
    calcavgsum(NodeList, [NumData|L]).

    
