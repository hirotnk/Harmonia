-module(hm_cache_test).
-compile(export_all).

start() ->
    hm_cache:start().

stop() ->
    hm_cache:stop([]).

store(N) ->
    store_in(N).

store_in(0) -> ok;
store_in(N) ->
    Key = "key" ++ integer_to_list(N),
    ok = hm_cache:store_cache(Key, {N, Key}),
    store_in(N-1).

get(N) ->
    get_in(N).

get_in(0) -> ok;
get_in(N) ->
    Key = "key" ++ integer_to_list(N),
    case hm_cache:get_cache(Key) of
        {ok, Rec} -> ok;
        none -> io:format("not found Key:[~p]~n", [Key])
    end,
    get_in(N-1).

