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

-module(hm_cli_demo).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-export([
        count_all/2,
        count_num/2,
        create_table8/1,
        rstore_dist_test_all/3,
        rstore_in_test_all/6,
        rstore_test_all/2
        ]).
-define(microsec, (1000*1000)).
-define(alph, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
-define(domain, "Domain8").
-include("harmonia.hrl").



create_table8(0) -> ok;
create_table8(N) ->
    Tbl   = "Tbl" ++ integer_to_list(N),
    % int(key), int(key), char(10,key), char(30)
    FldList = [{"Fld1",true,0},{"Fld2",true,0},{"Fld3",true,""}],
    hm_cli:create_table(?domain, Tbl, FldList),
    create_table8(N-1).

rstore_dist_test_all(NList, TNum, Len) ->
    rstore_dist_test_all_in(NList, 1, TNum, Len).

rstore_dist_test_all_in(_NList, _Nth, 0, _Len) -> ok;
rstore_dist_test_all_in(NList, Nth, TNum, Len) when Nth =:= length(NList) ->
    rstore_dist_test_all_in(NList, 1, TNum, Len);
rstore_dist_test_all_in(NList, Nth, TNum, Len) ->
    Node = lists:nth(Nth, NList),
    Tbl   = "Tbl" ++ integer_to_list(TNum),
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    {Fld3, _, _} = lists:nth(3, FldList),
    L = [[X]++[Y]++[Z] || X <- ?alph, Y <- ?alph, Z <- ?alph],
    rpc:cast(Node, hm_cli_demo, rstore_in_test_all, [L, length(L), Len, ?domain, Tbl, [Fld1, Fld2, Fld3]]),
    rstore_dist_test_all_in(NList, Nth + 1, TNum - 1, Len).


rstore_test_all(0, _Len) -> ok;
rstore_test_all(TNum, Len) ->
    Tbl   = "Tbl" ++ integer_to_list(TNum),
    FldList = [{"Fld1",true,true},{"Fld2",true,true},{"Fld3",false,nil}],
    {Fld1, _, _} = lists:nth(1, FldList),
    {Fld2, _, _} = lists:nth(2, FldList),
    {Fld3, _, _} = lists:nth(3, FldList),
    L = [[X]++[Y]++[Z] || X <- ?alph, Y <- ?alph, Z <- ?alph],
    rstore_in_test_all(L, length(L), Len, ?domain, Tbl, [Fld1,Fld2,Fld3]),
    rstore_test_all(TNum - 1, Len).

rstore_in_test_all(_, _, 0, _Domain, _Tbl, [_,_,_]) -> ok;
rstore_in_test_all(L, Range, Len, Domain, Tbl, [Fld1,Fld2,Fld3]) ->
    Candidate = lists:nth(random:uniform(Range), L),
    Text = "textfile" ++ integer_to_list(Len),
    hm_cli:rstore(Domain, Tbl, [{Fld1, Candidate},{Fld2, Len},{Fld3, Text}]),
    rstore_in_test_all(L, Range, Len - 1, Domain, Tbl, [Fld1,Fld2,Fld3]).


count_all(TNum) ->
    count_all_in(TNum, 0).

count_all_in(0, Cnt) -> 
    io:format("Total Record Count:[~p]\n", [Cnt]);
count_all_in(TNum, Cnt) ->
    Num = count_num(?domain, "Tbl" ++ integer_to_list(TNum)),
    count_all_in(TNum - 1, Cnt + Num).


count_num(Domain, Tbl) ->
    {ok, List} = hm_cli:rget(Domain, Tbl, "Fld2 >= 0"),
    Num = length(List),
    io:format("Domain:[~p] Table:[~p] Count:[~p]\n", [Domain, Tbl, Num]),
    Num.

