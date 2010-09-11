-module(hm_cli_test).
-export([
        get/1,
        cget/1,
        get_node_name/0,
        rget/1,
        cstore/1,
        rstore/1,
        make_table/0,
        store/1,
        test_all/0,
        test_all/1,
        test_perf/1
        ]).
-define(microsec, (1000*1000)).

test_all() ->
    test_all(10).

test_all(N) ->
    F = 
        fun({Format, Mod, Func, N, Ret}) ->
                Ret = Mod:Func(N),
                io:format(Format, [N]);
           ({Format, Func, N, Ret}) ->
                Func(N),
                io:format(Format, [N]);
            ({Format, Func, Ret}) when Ret =:= {ok, any} ->
                {ok, _} = Func(),
                io:format(Format);
           ({Format}) ->
                io:format(Format)
        end,
    lists:foreach(F, 
        [
            {"starting....\n"},
            {"store(~p)      OK....\n", fun store/1, N, ok},
            {"get(~p)        OK....\n", fun hm_cli_test:get/1, N, ok},
            {"cstore(~p)     OK....\n", fun cstore/1, N, ok}, 
            {"cget(~p)       OK....\n", fun cget/1,   N, ok},
            {"make_table()   OK....\n", fun make_table/0, {ok, any}},
            {"rstore(~p)     OK....\n", fun rstore/1, N, ok},
            {"rget(~p)       OK....\n", fun rget/1,   N, ok},
            {"..end\n"}
        ]
    ).

test_perf(N) ->
    F = 
        fun({Format, Mod, Func, N, Ret}) ->
                {Time, Ret} = timer:tc(Mod, Func, [N]),
                io:format(Format, [Time/?microsec]);

           ({Format, Func, N, Ret}) ->
               {Time, Ret} = timer:tc(?MODULE, Func, [N]),
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
            {"make_table    OK....[~20.10f] sec\n", make_table, {ok, any}},
            {"rstore        OK....[~20.10f] sec\n", rstore, N, ok},
            {"rget          OK....[~20.10f] sec\n", rget,   N, ok},
            {"..end\n"}
        ]
    ).

store(Len) -> store_in(Len).

cstore(Len) -> cstore_cache_in(Len).

get(Len) -> get_in(Len).

cget(Len) -> cget_in(Len).

make_table() ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    hm_cli:make_table(Domain, Tbl, FldList).

rstore(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    {Fld3, _, _} = lists:nth(3, FldList),
    rstore_in(Len, Domain, Tbl, [Fld1,Fld2,Fld3]).

rget(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    rget_in(Len, Domain, Tbl).

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

rstore_in(0, _Domain, _Tbl, [_,_,_]) -> ok;
rstore_in(Len, Domain, Tbl, [Fld1,Fld2,Fld3]) ->
    hm_cli:store(Domain, Tbl, [{Fld1, xxx},{Fld2, Len},{Fld3, textfile1}]),
    rstore_in(Len - 1, Domain, Tbl, [Fld1,Fld2,Fld3]).

rget_in(0, _Domain, _Tbl) -> ok;
rget_in(Len, Domain, Tbl) ->
    hm_cli:get(Domain, Tbl, "Fld1 == xxx and Fld2 == 33"),
    rget_in(Len - 1, Domain, Tbl).

get_node_name() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    {_Name, NodeName} = lists:nth(random:uniform(length(NameList)), NameList),
    {ok, NodeName}.
