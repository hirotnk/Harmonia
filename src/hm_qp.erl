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

%% Sts : Exp | ( 'or' Exp)*
%% Exp : Factor ( 'and' Factor)*
%% Factor : Term ( '=='|'!='|'<='|'>='|'>'|'<'  Term|Number|String )*
%%         | '(' Sts ')'
%% Term : Tbl.Fld
%% Tbl : StringNum
%% Fld : StringNum
%% StringNum : 'a-Z' ('a-Z'|'0-9')*
%% 
%% examples:
%% 
%% 
%% 
-module(hm_qp).
-compile([export_all]).

 -define(CHARACTERS, "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ_").
-define(NUMBERS, "0123456789").

parse(Tokens) ->
    {Result, []} = parse_sts(Tokens, []),
    [Result].

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
parse_term([{const,Val}|Tokens], []) -> 
    {{const, Val}, Tokens}.



scan(Query, AttList)->
    Tokens = scan(Query, [], []),
    lists:map(fun
                  ({identifier, String}) -> 
                      Token = lists:reverse(String),

                      case find_nth(Token, AttList) of 
                          {no_field, _} -> 
                              {identifier, list_to_atom(Token)};
                          N -> 
                              {identifier, list_to_atom("$" ++ integer_to_list(N+1))}
                      end;

                  ({const, String}) -> 
                      {const, list_to_integer(lists:reverse(String))};

                  ({atom, String}) -> 
                      {atom, list_to_atom(lists:reverse(String))};

                  ({Type, String}) -> 
                      {Type, String}
              end,
              Tokens).

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
scan("<=" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "<="}]);
scan("==" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "=="}]);
scan("!=" ++ Query, Cur, Tokens) ->
    scan(Query, [], Tokens++Cur++[{relational_operator, "!="}]);
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

eval({Op}) ->
    io:format("evaluating ~p ~n", [Op]),
    {result, Op};
eval({Rel, Op1, Op2}) ->
    Ret1 = eval(Op1),
    Ret2 = eval(Op2),
    io:format("evaluating ~p ~p ~p ==> ~p ~p ~p ~n", [Rel, Op1, Op2, Rel, Ret1, Ret2]),
    {result, Rel, Ret1, Ret2};
eval(Op) ->
    io:format("evaluating: ~p ~n", [Op]),
    {result, Op}.
    
find_nth(Token, AttList) -> find_nth_in(Token, AttList, 1).
find_nth_in(Token, [], _N) -> {no_field, Token};
find_nth_in(Token, [{FieldName, _, _} | AttList], N) ->
    case Token =:= FieldName of 
        true -> N;
        false -> 
            find_nth_in(Token, AttList, N+1)
    end.
