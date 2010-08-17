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
%%     A and (B or C) and B or D => {'or' {'and' {'and' A {'or' B C}} B} D}
%                                   {'or',{'and',{'and',f1,{'or',f2,f3}},f4},f5}
%%     A or (B or C) and B or D => {'or' A {'or' {'and' {'or' B C} B} D}}
-module(hm_qp).
-compile([export_all]).

-define(CHARACTERS, "abcdefghijklmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ_").
-define(NUMBERS, "0123456789").
-define(RESERVED_WORDS, ["or","and"]).
-define(RELATIONAL_OPERATORS, ["<",">","==","!=","<=",">="]).
-define(PAREN, "()"]).

parse(Tokens) ->
    {Result, []} = parse_sts(Tokens, []),
    Result.

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
        {relational_operator, _} -> 
            {Term2, Tokens3} = parse_term(Tokens2, []),
            {{term, {relational_operator, RelOp}, Term, Term2}, Tokens3};

        _ -> {error, parse_factor, Tokens2}
    end.


parse_term([{identifier,_}=Tble,{dot_op, _},{identifier, _}=Fld|Tokens], []) -> 
    {{table_field, Tble,  Fld}, Tokens};
parse_term([{identifier,Val}|Tokens], []) -> 
    {{string, Val}, Tokens};
parse_term([{number,Val}|Tokens], []) -> 
    {{number, Val}, Tokens}.



scan(Query)->
    scan(Query, [], []).

scan([],  Cur, Tokens) -> 
    lists:map(fun
                  ({identifier, String}) -> 
                      {identifier, lists:reverse(String)};
                  ({number, String}) -> 
                      {number, lists:reverse(String)};
                  ({Type, String}) -> 
                      {Type, String}
              end,
              Tokens++Cur);

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
    scan(Query, [], Tokens++Cur++[{left_paren, ")"}]);

scan([Char|Query], [], Tokens) ->
    case char_type(Char) of 
        character ->
            scan(Query, [{identifier, [Char]}], Tokens);
        number ->
            scan(Query, [{number, [Char]}], Tokens);
        _ ->
            error
    end;

scan([Char|Query], [{identifier, String}], Tokens) ->
    scan(Query, [{identifier, [Char|String]}], Tokens);
scan([Char|Query], [{number, String}], Tokens) ->
    case char_type(Char) of 
        number ->
            scan(Query, [{number, [Char|String]}], Tokens);
        _ -> error
    end.

char_type(Char) ->
    case lists:member(Char, ?CHARACTERS) of 
        true -> character;
        false ->
            case lists:member(Char, ?NUMBERS) of 
                true -> number;
                false -> other_char
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
    

