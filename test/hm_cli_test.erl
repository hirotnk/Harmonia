-module(hm_cli_test).
-export([
        get/1,
        store/1,
        make_table/0
        ]).

make_table() ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    hm_cli:make_table(Domain, Tbl, FldList).

store(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    {Fld3, _, _} = lists:nth(3, FldList),
    store_in(Len, Domain, Tbl, [Fld1,Fld2,Fld3]).

store_in(0, _Domain, _Tbl, [_,_,_]) -> ok;
store_in(Len, Domain, Tbl, [Fld1,Fld2,Fld3]) ->
    hm_cli:store(Domain, Tbl, [{Fld1, xxx},{Fld2, Len},{Fld3, textfile1}]),
    store_in(Len - 1, Domain, Tbl, [Fld1,Fld2,Fld3]).

get(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    get_in(Len, Domain, Tbl).

get_in(0, _Domain, _Tbl) -> ok;
get_in(Len, Domain, Tbl) ->
    Num = integer_to_list(Len),
    hm_cli:get(Domain, Tbl, "Fld1 == xxx and Fld2 == " ++ Num),
    get_in(Len - 1, Domain, Tbl).

