-module(hm_startup).
-export([
        start/0,
        stop/0
    ]).

start() ->
    case application:start(harmonia) of
        ok -> 
            io:format("harmonia application started.~n");
        {error, Msg} ->
            io:format("harmonia application start failed:~p~n", Msg)
    end.


stop() -> application:stop(harmonia).
