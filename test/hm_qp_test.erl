%% @author Yoshihiro TANAKA <hirotnkg@gmail.com>
%% 
%% @doc query scanner/parser test suite
%% 
%% to run:
%% erlc hm_qp_test.erl
%% erl -pa ../ebin -s hm_qp_test test -s init stop
%%
%% @end

-module(hm_qp_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

scan_test_() -> [
        ?_assertEqual([{identifier,'$2'},
                  {relational_operator, "=="},
                  {identifier, xxx}] , hm_qp:scan("Fld1 == xxx", [{"Fld1",true,true}])),
        ?_assertEqual([{identifier,'$2'},
                  {relational_operator,"=="},
                  {atom,'test test'}], hm_qp:scan("Fld1 == 'test test'",[{"Fld1",true,true}]))
  ].


