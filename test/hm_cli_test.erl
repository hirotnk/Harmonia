-module(hm_cli_test).
-export([
        rget/1,
        rstore/1,
        make_table/0
        ]).

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
    {ok, NodeName} = get_node_name(),
    rstore_in(NodeName, Len, Domain, Tbl, [Fld1,Fld2,Fld3]).

rstore_in(NodeName, 0, _Domain, _Tbl, [_,_,_]) -> ok;
rstore_in(NodeName, Len, Domain, Tbl, [Fld1,Fld2,Fld3]) ->
    %spawn(hm_cli, store, [NodeName, Domain, Tbl, [{Fld1, xxx},{Fld2, Len},{Fld3, textfile1}]]),
    hm_cli:store(NodeName, Domain, Tbl, [{Fld1, xxx},{Fld2, Len},{Fld3, textfile1}]),
    rstore_in(NodeName, Len - 1, Domain, Tbl, [Fld1,Fld2,Fld3]).

rget(Len) ->
    Domain = "Domain1",
    Tbl   = "Tbl2",
    {ok, NodeName} = get_node_name(),
    rget_in(NodeName, Len, Domain, Tbl).

rget_in(NodeName, 0, _Domain, _Tbl) -> ok;
rget_in(NodeName, Len, Domain, Tbl) ->
    %Num = integer_to_list(Len),
    %spawn(hm_cli, get, [NodeName, Domain, Tbl, "Fld1 == xxx and Fld2 == " ++ Num]),
    %hm_cli:get(NodeName, Domain, Tbl, "Fld1 == xxx and Fld2 == " ++ Num),
    hm_cli:get(NodeName, Domain, Tbl, "Fld1 == xxx and Fld2 == 33"),
    rget_in(NodeName, Len - 1, Domain, Tbl).

get_node_name() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    {_Name, NodeName} = lists:nth(random:uniform(length(NameList)), NameList),
    {ok, NodeName}.

