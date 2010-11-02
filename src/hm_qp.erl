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
%%%-------------------------------------------------------------------
%%% @author Yoshihiro TANAKA <hirotnkg@gmail.com>
%%% @copyright (C) 2010, hiro
%%% @doc query parser for ets
%%%      convert query condition into match specification of ets
%%%
%%%      Sts    = Exp , {'or' Exp};
%%%      Exp    = Factor , {'and' Factor};
%%%      Factor = Fname RelOp  Term | '(' , Sts , ')';
%%%      Fname  = Char , {Char|Number};
%%%      Term   = Atom|Number|String;
%%%      RelOp  = '=='|'!='|'<='|'>='|'>'|'<';
%%%      Number = Digit , {Digit};
%%%      String = '[' , {Char|Digit} , ']';
%%%      Atom   = ''' , {Char|Digit} , ''';
%%%      Digit  = '0-9';
%%%      Char   = 'a-Z';
%%%
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro TANAKA <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_qp).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-vsn('0.1').
%% API
-export([
        eval/1,
        parse/1,
        scan/2,
        scan/3
        ]).
-include_lib("eunit/include/eunit.hrl").

-define(CHARACTERS, "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ_").
-define(NUMBERS, "0123456789").

%%%===================================================================
%%% API
%%%===================================================================
parse(Tokens) ->
    {Result, []} = parse_sts(Tokens, []),
    [Result].

eval({Op}) ->
    {result, Op};
eval({Rel, Op1, Op2}) ->
    Ret1 = eval(Op1),
    Ret2 = eval(Op2),
    {result, Rel, Ret1, Ret2};
eval(Op) ->
    {result, Op}.

scan(Query, {IsKey, AttList})->
    Tokens = scan(Query, [], []),
    scan_ret(Tokens, {IsKey, AttList}).

scan([],  Cur, Tokens) -> Tokens ++ Cur;

% scan atom
scan([$'|Query], [], Tokens) ->
    scan(Query, [{atom, []}], Tokens);
scan([$'|Query], [{atom, Cur}], Tokens) ->
    scan(Query, [], Tokens++[{atom, Cur}]);
scan([$'|Query], Cur, Tokens) ->
    scan(Query, [{atom, []}], Tokens++Cur);
scan([Char|Query], [{atom,Cur}], Tokens) ->
    scan(Query, [{atom, [Char|Cur]}], Tokens);

% scan string
scan([$[|Query], [], Tokens) ->
    scan(Query, [{string, []}], Tokens);
scan([$]|Query], [{string, Cur}], Tokens) ->
    scan(Query, [], Tokens++[{string, lists:reverse(Cur)}]);
scan([$[|Query], Cur, Tokens) ->
    scan(Query, [{string, []}], Tokens++Cur);
scan([Char|Query], [{string,Cur}], Tokens) ->
    scan(Query, [{string, [Char|Cur]}], Tokens);


% general cases
scan(" " ++ Query, [], Tokens) ->
    scan(Query, [], Tokens);

scan(" " ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur);

scan("and" ++ Query, [], Tokens) ->
    scan(Query, [], Tokens++[{and_op, "and"}]);
scan("or" ++ Query, [], Tokens) ->
    scan(Query, [], Tokens++[{or_op, "or"}]);

scan(">=" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, ">="}]);
scan("=<" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "=<"}]);
scan("=>" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, ">="}]);
scan("<=" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "=<"}]);
scan("==" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "=="}]);
scan("!=" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "/="}]);
scan("<" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "<"}]);
scan(">" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, ">"}]);

scan("." ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{dot_op, "."}]);

scan("(" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{left_paren, "("}]);
scan(")" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{right_paren, ")"}]);

scan([Char|Query], [], Tokens) ->
    case char_type(Char) of 
        character ->
            scan(Query, [{identifier, [Char]}], Tokens);
        number ->
            scan(Query, [{const, [Char]}], Tokens);
        _ ->
            error
    end;

scan([Char|Query], [{identifier, String}], Tokens) ->
    scan(Query, [{identifier, [Char|String]}], Tokens);
scan([Char|Query], [{const, String}], Tokens) ->
    case char_type(Char) of 
        number ->
            scan(Query, [{const, [Char|String]}], Tokens);
        _ -> error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
scan_ret(Tokens, {IsKey, AttList}) -> 
    scan_ret_in(Tokens, {IsKey, AttList}, []).

scan_ret_in([], {_IsKey, _AttList}, Result) -> lists:reverse(Result);
scan_ret_in([{identifier, String}|Tokens], {IsKey, AttList}, Result) when IsKey =:= index ->
    Token = lists:reverse(String),
    case find_nth(Token, AttList) of 
        {no_field, _} -> 
            scan_ret_in(
                Tokens,
                {IsKey, AttList},
                [{identifier, list_to_atom(Token)}|Result]
            );

        {false, _N} -> 
            %% if the field name is NOT key field, then ignore this field condition
            %% for key table search
            {_Discard, NewTokens} = lists:split(2,Tokens),
            scan_ret_in(
                NewTokens, 
                {IsKey, AttList},
                [{atom, true},{relational_operator,"=:="},{atom, true}|Result]
            );

        {true, N} -> 
            %% add 1, because $1 is the table name
            scan_ret_in(
                Tokens,
                {IsKey, AttList},
                [{identifier, list_to_atom("$" ++ integer_to_list(N+1))}|Result]
            )
    end;
scan_ret_in([{identifier, String}|Tokens], {IsKey, AttList}, Result) ->
    Token = lists:reverse(String),
    NewToken = 
        case find_nth(Token, AttList) of 
            {no_field, _} -> 
                {identifier, list_to_atom(Token)};
            {_, N} -> 
                %% add 1, because $1 is the table name
                {identifier, list_to_atom("$" ++ integer_to_list(N+1))}
        end,
    scan_ret_in(Tokens, {IsKey, AttList}, [NewToken|Result]);
scan_ret_in([{const, String}|Tokens], {IsKey, AttList}, Result) ->
    NewToken = {const, list_to_integer(lists:reverse(String))},
    scan_ret_in(Tokens, {IsKey, AttList}, [NewToken|Result]);
scan_ret_in([{atom, String}|Tokens], {IsKey, AttList}, Result) ->
    NewToken = {atom, list_to_atom(lists:reverse(String))},
    scan_ret_in(Tokens, {IsKey, AttList}, [NewToken|Result]);
scan_ret_in([{Type, String}|Tokens], {IsKey, AttList}, Result) ->
    NewToken = {Type, String},
    scan_ret_in(Tokens, {IsKey, AttList}, [NewToken|Result]).

char_type(Char) ->
    case lists:member(Char, ?CHARACTERS) of 
        true -> character;
        false ->
            case lists:member(Char, ?NUMBERS) of 
                true -> number;
                false -> 
                    case Char of 
                        $' -> single_quote;
                        $" -> double_quote;
                        _  -> other_char
                    end
            end
    end.

find_nth(Token, AttList) -> 
    find_nth_in(Token, AttList, 1).

find_nth_in(Token, [], _N) -> {no_field, Token};
find_nth_in(Token, [{FieldName, IsKey, _} | AttList], N) ->
    case Token =:= FieldName of 
        true -> {IsKey, N};
        false -> 
            find_nth_in(Token, AttList, N+1)
    end.

parse_sts(Tokens, []) ->
    case parse_exp(Tokens, []) of
        {Exp1, [{or_op,_}|Tokens2]} ->
                    {Exp2, Tokens3} = parse_sts(Tokens2,[]),
                    {{'or', Exp1, Exp2}, Tokens3};
        {Exp1, [{right_paren,_}|Tokens2]} ->
                {Exp1, Tokens2};
        {Exp1, []} -> {Exp1, []};
        _ -> {error, parse_sts, Tokens}
    end.

parse_exp(Tokens, []) ->
    case parse_factor(Tokens, []) of
        {Factor1, [{and_op,_}|Tokens2]} ->
                {Factor2, Tokens3} = parse_exp(Tokens2,[]),
                {{'and', Factor1, Factor2}, Tokens3};
        {Factor1, Tokens1} -> {Factor1, Tokens1};
        _ -> {error, parse_exp, Tokens}
    end.

parse_factor([{left_paren, _} | Tokens], []) -> 
    {Exp, Tokens2} = parse_sts(Tokens, []),
    {Exp, Tokens2};
parse_factor(Tokens, []) -> 
    {Term, [RelOp|Tokens2]} = parse_term(Tokens, []),
    case RelOp of 
        {relational_operator, Op} -> 
            {Term2, Tokens3} = parse_term(Tokens2, []),
            AtomOp = list_to_atom(Op),
            {{AtomOp, Term, Term2}, Tokens3};

        _ -> {error, parse_factor, Tokens2}
    end.

parse_term([{identifier,_}=Tble,{dot_op, _},{identifier, _}=Fld|Tokens], []) -> 
    {{table_field, Tble,  Fld}, Tokens};
parse_term([{identifier,Val}|Tokens], []) -> 
    {Val, Tokens};
parse_term([{atom,Val}|Tokens], []) -> 
    {Val, Tokens};
parse_term([{string,Val}|Tokens], []) -> 
    {Val, Tokens};
parse_term([{const,Val}|Tokens], []) -> 
    {{const, Val}, Tokens}.

%%%===================================================================
%%% EUnit test functions
%%%===================================================================

rangeq_test_() -> 
    [
        ?_assertEqual({ok, [[xxx,32,textfile1]]}   ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 32")),
        ?_assertEqual({ok, [[yyy,150,textfile2]]}  ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 150")),
        ?_assertEqual({ok, [[zzz,3000,textfile3]]} ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 3000")),
        ?_assertEqual({ok, [[aaa,9000,textfile4]]} ,hm_cli:get("Domain1", "Tbl2", "Fld2 == 9000"))
    ].
