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

-module(hm_cache).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-vsn('0.1').

-export([
        get_cache/1,
        del_cache/1,
        start/0,
        store_cache/2
    ]).

-include("harmonia.hrl").

start() ->
    ets:new(?hm_ets_cache_table, [named_table, public]).

store_cache(Key, Value) ->
    {MegaSecs, Secs, _Microsecs} = now(),
    ets:insert(?hm_ets_cache_table, 
               {Key, Value, 0, MegaSecs*1000000 + Secs + ?cache_timeout}),
    ok.

%% @spec(get_cache(Key::list()|atom()|integer()) -> none|{ok, {Value::term(), Cnt::integer()}}).
get_cache(Key) ->
    case ets:lookup(?hm_ets_cache_table, Key) of
        [] -> none;
        [{Key, Value, Cnt, _}] -> 
            % {"key10000",{10000,"key10000"},0,1283991518}
           {MegaSecs, Secs, _Microsecs} = now(),
           ets:update_element(
               ?hm_ets_cache_table, 
               Key, 
               [{3,Cnt+1},{4, MegaSecs*1000000 + Secs + ?cache_timeout}]
           ),
           {ok, {Value, Cnt}}
    end.


%% @spec(del_cache(Key::list()|atom()|integer()) -> {ok, Key}).
del_cache(Key) ->
    ets:delete(?hm_ets_cache_table, Key),
    {ok, Key}.
