%% start shell command:
%%  root node:
%%    erl -pa ebin -config harmonia -harmonia type 'create' -harmonia name foo
%%  join node:
%%    erl -pa ebin -config harmonia -harmonia type 'join' -harmonia name <name> - harmonia root <root name>
%%
%%

-module(harmonia).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case hm_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.

stop(_State) ->
    ok.

