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

-module(hm_cli_test).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-export([
        cget/1,
        cget_cond/1,
        create_table/0,
        create_table_test_all/0,
        cstore/1,
        drop_table/0,
        get/1,
        get_cond/1,
        get_node_name/0,
        rangeq_test0/0,
        rangeq_test1/0,
        rangeq_test2/0,
        rangeq_test3/0,
        rangeq_test4/0,
        rangeq_test5/0,
        rangeq_test_all/0,
        rget/1,
        rget/2,
        rget_cond/1,
        rget_test_all/1,
        rstore/1,
        store/1,
        test_all/0,
        test_all/1,
        test_comp_get/1,
        test_perf/1
        ]).
-define(microsec, (1000*1000)).

-include("harmonia.hrl").

-include_lib("eunit/include/eunit.hrl").



test_all() ->
    test_all(10),
    rangeq_test_all().

test_all(N) ->
    F = 
        fun({Format, Mod, Func, K, Ret}) ->
                Ret = Mod:Func(K),
                io:format(Format, [K]);
           ({Format, Func, K, _Ret}) ->
                Func(K),
                io:format(Format, [K]);
           ({Format, Func, Ret}) when Ret =:= {ok, any} ->
                case Func() of
                    {ok, _} -> io:format(Format,["OK"]);
                    Msg -> io:format(Format,[Msg])
                end;
           ({Format}) ->
                io:format(Format)
        end,
    lists:foreach(F, 
        [
            {"starting....\n"},
            {"store(~p)      OK....\n", fun store/1,           N, ok},
            {"get(~p)        OK....\n", fun hm_cli_test:get/1, N, ok},
            {"cstore(~p)     OK....\n", fun cstore/1,          N, ok}, 
            {"cget(~p)       OK....\n", fun cget/1,            N, ok},
            {"drop_table()   ~p....\n", fun drop_table/0,    {ok, any}},
            {"create_table() ~p....\n", fun create_table_test_all/0,  {ok, any}},
            {"rstore(~p)     OK....\n", fun rstore_test_all/1,          N, ok},
            {"rget(~p)       OK....\n", fun rget_test_all/1,            N, ok},
            {"..end\n"}
        ]
    ),
    rangeq_test_all().

test_perf(N) ->
    F = 
        fun({Format, Mod, Func, K, Ret}) ->
                {Time, Ret} = timer:tc(Mod, Func, [K]),
                io:format(Format, [Time/?microsec]);

           ({Format, Func, K, Ret}) ->
               {Time, Ret} = timer:tc(?MODULE, Func, [K]),
                io:format(Format, [Time/?microsec]);

           ({Format, Func, Ret}) when Ret =:= {ok, any} ->
                {Time, {ok, _}} = timer:tc(?MODULE, Func, []),
                io:format(Format, [Time/?microsec]);

           ({Format}) ->
                io:format(Format)
        end,
    lists:foreach(F, 
        [
            {"starting....\n"},
            {"store         OK....[~20.10f] sec\n", store, N, ok},
            {"get           OK....[~20.10f] sec\n", get, N, ok},
            {"cstore        OK....[~20.10f] sec\n", cstore, N, ok}, 
            {"cget          OK....[~20.10f] sec\n", cget,   N, ok},
            {"drop_table()  OK....[~20.10f] sec\n", drop_table,   {ok, any}},
            {"create_table  OK....[~20.10f] sec\n", create_table, {ok, any}},
            {"rstore        OK....[~20.10f] sec\n", rstore, N, ok},
            {"rget          OK....[~20.10f] sec\n", rget,   N, ok},
            {"..end\n"}
        ]
    ).

test_comp_get(R) ->
    F = 
        fun
            ({Format, Func, {_,_,_,Last}=K}) ->
               {Time, _} = timer:tc(?MODULE, Func, [K]),
               io:format(Format, [Last, Time/?microsec]);
            ({Format, Func, K, Last}) ->
               {Time, _} = timer:tc(?MODULE, Func, [K]),
               %io:format("~p~n", [Ret]),
               io:format(Format, [Last, Time/?microsec]);
           ({Format}) ->
               io:format(Format)
        end,
    lists:foreach(F, 
        [
            {"starting....\n"},
            {"get   Between 1 and 1,    in ~p     OK....[~20.10f] sec\n", get_cond, {1,1,1,R}},
            {"cget  Between 1 and 1,    in ~p     OK....[~20.10f] sec\n", cget_cond, {1,1,1,R}},
            {"rget  Between 1 and 1,    in ~p     OK....[~20.10f] sec\n", rget_cond, {1,1}, R},
            {"get   Between 1 and 10,   in ~p     OK....[~20.10f] sec\n", get_cond, {1,10,1,R}},
            {"cget  Between 1 and 10,   in ~p     OK....[~20.10f] sec\n", cget_cond, {1,10,1,R}},
            {"rget  Between 1 and 10,   in ~p     OK....[~20.10f] sec\n", rget_cond, {1,10}, R},
            {"get   Between 1 and 100,  in ~p     OK....[~20.10f] sec\n", get_cond, {1,100,1,R}},
            {"cget  Between 1 and 100,  in ~p     OK....[~20.10f] sec\n", cget_cond, {1,100,1,R}},
            {"rget  Between 1 and 100,  in ~p     OK....[~20.10f] sec\n", rget_cond, {1,100}, R},
            {"get   Between 1 and 500,  in ~p     OK....[~20.10f] sec\n", get_cond, {1,500,1,R}},
            {"cget  Between 1 and 500,  in ~p     OK....[~20.10f] sec\n", cget_cond, {1,500,1,R}},
            {"rget  Between 1 and 500,  in ~p     OK....[~20.10f] sec\n", rget_cond, {1,500}, R},
            {"..end\n"}
        ]
    ).

get_cond({Min, Max, From, To}) ->
    get_cond_in(Min,Max,From,To,[]).

get_cond_in(_Min,_Max,From,To, L) when From > To -> L;
get_cond_in(Min,Max,From,To, L) ->
    {ok,[{Key,Dat}]} = hm_cli:get(From),
    NewList = 
        case (Key >= Min) and (Key =< Max) of
            true -> [{Key,Dat}|L];
            false -> L
        end,
    get_cond_in(Min, Max, From + 1, To, NewList).


cget_cond({Min, Max, From, To}) ->
    cget_cond_in(Min,Max,From,To,[]).

cget_cond_in(_Min,_Max,From,To, L) when From > To -> L;
cget_cond_in(Min,Max,From,To, L) ->
    {ok,[{Key,Dat}]} = hm_cli:cget(From),
    NewList = 
        case (Key >= Min) and (Key =< Max) of
            true -> [{Key,Dat}|L];
            false -> L
        end,
    cget_cond_in(Min, Max, From + 1, To, NewList).

rget_cond({Min,Max}) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    hm_cli:rget(Domain, Tbl, "Fld2 >= " ++ integer_to_list(Min) ++ " and Fld2 <= " ++ integer_to_list(Max)).

store(Len) -> store_in(Len).

cstore(Len) -> cstore_cache_in(Len).

get(Len) -> get_in(Len).

cget(Len) -> cget_in(Len).

create_table_test_all() ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    hm_cli:create_table(Domain, Tbl, FldList).

create_table() ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true}],
    hm_cli:create_table(Domain, Tbl, FldList).

drop_table() ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    hm_cli:drop_table(Domain, Tbl).

rstore_test_all(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    {Fld3, _, _} = lists:nth(3, FldList),
    rstore_in_test_all(Len, Domain, Tbl, [Fld1,Fld2,Fld3]).

rstore(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    rstore_in(Len, Domain, Tbl, [Fld1,Fld2]).

rget_test_all(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    rget_in_test_all(Len, Domain, Tbl).

rget(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    rget_in(Len, Domain, Tbl).

rget(Start, End) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    rget_in(Start, End, Domain, Tbl).

rangeq_test_all() ->
    %% TODO: here, you need to delete all records.
    rangeq_test0(),
    rangeq_test1(),
    rangeq_test2(),
    rangeq_test3(),
    rangeq_test4(),
    rangeq_test5().

rangeq_test0() -> 
    Result1 = {ok, ?succ_list_len + 1},
    L1 = [{"Fld1", xxx},{"Fld2", 32},{"Fld3", textfile1}],
    L2 = [{"Fld1", yyy},{"Fld2", 150},{"Fld3", textfile2}],
    L3 = [{"Fld1", zzz},{"Fld2", 3000},{"Fld3", textfile3}],
    L4 = [{"Fld1", aaa},{"Fld2", 9000},{"Fld3", textfile4}],

    ?assertEqual(Result1, hm_cli:rstore("Domain1", "Tbl2", L1)),
    ?assertEqual(Result1, hm_cli:rstore("Domain1", "Tbl2", L2)),
    ?assertEqual(Result1, hm_cli:rstore("Domain1", "Tbl2", L3)),
    ?assertEqual(Result1, hm_cli:rstore("Domain1", "Tbl2", L4)).

rangeq_test1() -> 
    io:format("rangeq_test1 start~n"),
    D = "Domain1", T = "Tbl2", 
    Q1 = "Fld2 == 32",
    Q2 = "Fld2 == 150",
    Q3 = "Fld2 == 3000",
    Q4 = "Fld2 == 9000",

    ?assertEqual({ok, [[xxx,32,textfile1]]}   ,hm_cli:rget(D, T, Q1)),
    io:format("[~p ~p ~p ~p]:ok~n",["case1", D,T,Q1]),
    ?assertEqual({ok, [[yyy,150,textfile2]]}  ,hm_cli:rget(D, T, Q2)),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q2]),
    ?assertEqual({ok, [[zzz,3000,textfile3]]} ,hm_cli:rget(D, T, Q3)),
    io:format("[~p ~p ~p ~p]:ok~n",["case3",D,T,Q3]),
    ?assertEqual({ok, [[aaa,9000,textfile4]]} ,hm_cli:rget(D, T, Q4)),
    io:format("[~p ~p ~p ~p]:ok~n",["case4",D,T,Q4]).

rangeq_test2() -> 
    io:format("rangeq_test2 start~n"),
    {ok, RowList1} = hm_cli:rget(D = "Domain1", T = "Tbl2", Q1 = "Fld2 != 32"),
    lists:foreach(fun([_,N,_]) -> ?assert(N =/= 32) end, RowList1),
    io:format("[~p ~p ~p ~p]:ok~n",["case1",D,T,Q1]),

    {ok, RowList2} = hm_cli:rget(D = "Domain1", T = "Tbl2", Q2 = "Fld2 != 150"),
    lists:foreach(fun([_,N,_]) -> ?assert(N =/= 150)  end, RowList2),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q2]),

    {ok, RowList3} = hm_cli:rget(D = "Domain1", T = "Tbl2", Q3 = "Fld2 != 3000"),
    lists:foreach(fun([_,N,_]) -> ?assert(N =/= 3000) end, RowList3),
    io:format("[~p ~p ~p ~p]:ok~n",["case3",D,T,Q3]),

    {ok, RowList4} = hm_cli:rget(D = "Domain1", T = "Tbl2", Q4 = "Fld2 != 9000"),
    lists:foreach(fun([_,N,_]) -> ?assert(N =/= 9000) end, RowList4),
    io:format("[~p ~p ~p ~p]:ok~n",["case4",D,T,Q4]).

rangeq_test3() -> 
    io:format("rangeq_test3 start~n"),
    D = "Domain1", T = "Tbl2", 
    Q1 = "Fld2 > 32",
    Q2 = "Fld2 > 150",
    Q3 = "Fld2 > 3000",
    Q4 = "Fld2 > 9000",
    Q5 = "Fld2 < 32",
    Q6 = "Fld2 < 150",
    Q7 = "Fld2 < 3000",
    Q8 = "Fld2 < 9000",

    {ok, RowList1} = hm_cli:rget(D, T, Q1),
    lists:foreach(fun([_,N,_]) -> ?assert(N > 32) end, RowList1),
    io:format("[~p ~p ~p ~p]:ok~n",["case1",D,T,Q1]),
    {ok, RowList2} = hm_cli:rget(D, T, Q2),
    lists:foreach(fun([_,N,_]) -> ?assert(N > 150) end, RowList2),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q2]),
    {ok, RowList3} = hm_cli:rget(D, T, Q3),
    lists:foreach(fun([_,N,_]) -> ?assert(N > 3000) end, RowList3),
    io:format("[~p ~p ~p ~p]:ok~n",["case3",D,T,Q3]),
    {ok, RowList4} = hm_cli:rget(D, T, Q4),
    lists:foreach(fun([_,N,_]) -> ?assert(N > 9000) end, RowList4),
    io:format("[~p ~p ~p ~p]:ok~n",["case4",D,T,Q4]),

    {ok, RowList5} = hm_cli:rget(D, T, Q5),
    lists:foreach(fun([_,N,_]) -> ?assert(N < 32) end, RowList5),
    io:format("[~p ~p ~p ~p]:ok~n",["case5",D,T,Q5]),
    {ok, RowList6} = hm_cli:rget(D, T, Q6),
    lists:foreach(fun([_,N,_]) -> ?assert(N < 150) end, RowList6),
    io:format("[~p ~p ~p ~p]:ok~n",["case6",D,T,Q6]),
    {ok, RowList7} = hm_cli:rget(D, T, Q7),
    lists:foreach(fun([_,N,_]) -> ?assert(N < 3000) end, RowList7),
    io:format("[~p ~p ~p ~p]:ok~n",["case7",D,T,Q7]),
    {ok, RowList8} = hm_cli:rget(D, T, Q8),
    lists:foreach(fun([_,N,_]) -> ?assert(N < 9000) end, RowList8),
    io:format("[~p ~p ~p ~p]:ok~n",["case8",D,T,Q8]).

rangeq_test4() -> 
    io:format("rangeq_test4 start~n"),
    D = "Domain1", T = "Tbl2", 
    Q1 = "Fld2 >= 32",
    Q2 = "Fld2 >= 150",
    Q3 = "Fld2 >= 3000",
    Q4 = "Fld2 >= 9000",
    Q5 = "Fld2 <= 32",
    Q6 = "Fld2 <= 150",
    Q7 = "Fld2 <= 3000",
    Q8 = "Fld2 <= 9000",

    {ok, RowList1} = hm_cli:rget(D, T, Q1),
    lists:foreach(fun([_,N,_]) -> ?assert(N >= 32) end, RowList1),
    io:format("[~p ~p ~p ~p]:ok~n",["case1",D,T,Q1]),
    {ok, RowList2} = hm_cli:rget(D, T, Q2),
    lists:foreach(fun([_,N,_]) -> ?assert(N >= 150) end, RowList2),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q2]),
    {ok, RowList3} = hm_cli:rget(D, T, Q3),
    lists:foreach(fun([_,N,_]) -> ?assert(N >= 3000) end, RowList3),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q3]),
    {ok, RowList4} = hm_cli:rget(D, T, Q4),
    lists:foreach(fun([_,N,_]) -> ?assert(N >= 9000) end, RowList4),
    io:format("[~p ~p ~p ~p]:ok~n",["case3",D,T,Q4]),

    {ok, RowList5} = hm_cli:rget(D, T, Q5),
    lists:foreach(fun([_,N,_]) -> ?assert(N =< 32) end, RowList5),
    io:format("[~p ~p ~p ~p]:ok~n",["case4",D,T,Q5]),
    {ok, RowList6} = hm_cli:rget(D, T, Q6),
    lists:foreach(fun([_,N,_]) -> ?assert(N =< 150) end, RowList6),
    io:format("[~p ~p ~p ~p]:ok~n",["case5",D,T,Q6]),
    {ok, RowList7} = hm_cli:rget(D, T, Q7),
    lists:foreach(fun([_,N,_]) -> ?assert(N =< 3000) end, RowList7),
    io:format("[~p ~p ~p ~p]:ok~n",["case6",D,T,Q7]),
    {ok, RowList8} = hm_cli:rget(D, T, Q8),
    lists:foreach(fun([_,N,_]) -> ?assert(N =< 9000) end, RowList8),
    io:format("[~p ~p ~p ~p]:ok~n",["case7",D,T,Q8]).



rangeq_test5() -> 
    io:format("rangeq_test5 start~n"),
    D = "Domain1", T = "Tbl2", 
    Q1 = "Fld2 >= 32 and Fld2 <= 150",
    Q2 = "Fld2 >= 3000 or Fld2 <= 150",
    Q3 = "(Fld2 >= 32 and Fld2 <= 150) and Fld3 == textfile1",
    Q4 = "Fld2 >= 3000 and Fld2 <= 150",
    Q5 = "Fld1 == yyy and Fld2 == 150",
    Q6 = "Fld1 == yyy or Fld2 == 32",
    Q7 = "Fld1 == yyy and Fld2 == 150",

    {ok, RowList1} = hm_cli:rget(D, T, Q1),
    lists:foreach(fun([_,N,_]) -> ?assert((N >= 32) and (N =< 150)) end, RowList1),
    io:format("[~p ~p ~p ~p]:ok~n",["case1",D,T,Q1]),

    {ok, RowList2} = hm_cli:rget(D, T, Q2),
    lists:foreach(fun([_,N,_]) -> ?assert((N >= 3000) or (N =< 150)) end, RowList2),
    io:format("[~p ~p ~p ~p]:ok~n",["case2",D,T,Q2]),

    {ok, RowList3} = hm_cli:rget(D, T, Q3),
    lists:foreach(fun([_,N,O]) -> ?assert(((N >= 3000) or (N =< 150)) and (O =:= textfile1)) end, RowList3),
    io:format("[~p ~p ~p ~p]:ok~n",["case3",D,T,Q3]),

    {ok, RowList4} = hm_cli:rget(D, T, Q4),
    lists:foreach(fun([_,N,_]) -> ?assert(((N >= 3000) and (N =< 150))) end, RowList4),
    io:format("[~p ~p ~p ~p]:ok~n",["case4",D,T,Q4]),

    {ok, RowList5} = hm_cli:rget(D, T, Q5),
    lists:foreach(fun([M,N,_]) -> ?assert(((M =:= yyy) and (N =:= 150))) end, RowList5),
    io:format("[~p ~p ~p ~p]:ok~n",["case5",D,T,Q5]),

    {ok, RowList6} = hm_cli:rget(D, T, Q6),
    lists:foreach(fun([M,N,_]) -> ?assert(((M =:= yyy) or (N =:= 32))) end, RowList6),
    io:format("[~p ~p ~p ~p]:ok~n",["case6",D,T,Q6]),

    {ok, RowList7} = hm_cli:rget(D, T, Q7),
    lists:foreach(fun([M,N,_O]) -> ?assert((M =:= yyy) and (N =:= 150)) end, RowList7),
    io:format("[~p ~p ~p ~p]:ok~n",["case7",D,T,Q7]).

%% ----------------------------------------------------------------------------
%% Internal Functions
%% ----------------------------------------------------------------------------
store_in(0) -> ok;
store_in(Len) when is_integer(Len) ->
    Val = Len + 100,
    hm_cli:store(Len, Val),
    store_in(Len-1).

cstore_cache_in(0) -> ok;
cstore_cache_in(Len) when is_integer(Len) ->
    Val = Len + 100,
    hm_cli:cstore(Len, Val),
    cstore_cache_in(Len-1).

get_in(0) -> ok;
get_in(Len) ->
    hm_cli:get(Len),
    get_in(Len-1).

cget_in(0) -> ok;
cget_in(Len) ->
    hm_cli:cget(Len),
    cget_in(Len-1).

rstore_in_test_all(0, _Domain, _Tbl, [_,_,_]) -> ok;
rstore_in_test_all(Len, Domain, Tbl, [Fld1,Fld2,Fld3]) ->
    hm_cli:rstore(Domain, Tbl, [{Fld1, xxx},{Fld2, Len},{Fld3, textfile1}]),
    rstore_in_test_all(Len - 1, Domain, Tbl, [Fld1,Fld2,Fld3]).

rstore_in(0, _Domain, _Tbl, [_,_]) -> ok;
rstore_in(Len, Domain, Tbl, [Fld1,Fld2]) ->
    hm_cli:rstore(Domain, Tbl, [{Fld1, xxx},{Fld2, Len}]),
    rstore_in(Len - 1, Domain, Tbl, [Fld1,Fld2]).

rget_in_test_all(0, _Domain, _Tbl) -> ok;
rget_in_test_all(Len, Domain, Tbl) ->
    {ok, [[xxx,Len,_]]} = hm_cli:rget(Domain, Tbl, "Fld2 == " ++ integer_to_list(Len)),
    rget_in_test_all(Len - 1, Domain, Tbl).

rget_in(0, _Domain, _Tbl) -> ok;
rget_in(Len, Domain, Tbl) ->
    {ok, [[xxx,Len]]} = hm_cli:rget(Domain, Tbl, "Fld2 == " ++ integer_to_list(Len)),
    rget_in(Len - 1, Domain, Tbl).

rget_in(Start, End, _Domain, _Tbl) when Start > End -> ok;
rget_in(Start, End, Domain, Tbl) ->
    {ok, [[xxx,Start]]} = hm_cli:rget(Domain, Tbl, "Fld2 == " ++ integer_to_list(Start)),
    rget_in(Start + 1, End, Domain, Tbl).

get_node_name() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    {_Name, NodeName} = lists:nth(random:uniform(length(NameList)), NameList),
    {ok, NodeName}.

