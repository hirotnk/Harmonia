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
          % simple scan test
        ?_assertEqual([{identifier,'$2'},
                  {relational_operator, "=="},
                  {identifier, xxx}] , hm_qp:scan("Fld1 == xxx", [{"Fld1",true,true}])),

          % atom test
        ?_assertEqual([{identifier,'$2'},
                  {relational_operator,"=="},
                  {atom,'test test'}], hm_qp:scan("Fld1 == 'test test'",[{"Fld1",true,true}])),

          % no space between == and atom 'test test'
        ?_assertEqual([{identifier,'$2'},
                  {relational_operator,"=="},
                  {atom,'test test'}], hm_qp:scan("Fld1 =='test test'",[{"Fld1",true,true}]))
  ].


