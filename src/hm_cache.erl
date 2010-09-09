-module(hm_cache).
-vsn('0.1').

-export([
        get_cache/1,
        start/0,
        store_cache/2
    ]).

-include("harmonia.hrl").

start() ->
    ets:new(?ets_cache_table, [named_table, public]).

store_cache(Key, Value) ->
    {MegaSecs, Secs, _Microsecs} = now(),
    Ret = (
            catch 
                ets:insert(
                    ?ets_cache_table, 
                    {Key, Value, 0, MegaSecs*1000000 + Secs + ?cache_timeout}
                )
          ),
    ok.

-spec(get_cache(Key::list()|atom()|integer()) -> none|{ok, {Value::term(), Cnt::integer()}}).
get_cache(Key) ->
    case ets:lookup(?ets_cache_table, Key) of
        [] -> none;
        [{Key, Value, Cnt, _}] -> 
            % {"key10000",{10000,"key10000"},0,1283991518}
           {MegaSecs, Secs, _Microsecs} = now(),
           ets:update_element(
               ?ets_cache_table, 
               Key, 
               [{3,Cnt+1},{4, MegaSecs*1000000 + Secs + ?cache_timeout}]
           ),
           {ok, {Value, Cnt}}
    end.


