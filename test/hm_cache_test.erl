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

-module(hm_cache_test).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
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
        {ok, _Rec} -> ok;
        none -> io:format("not found Key:[~p]~n", [Key])
    end,
    get_in(N-1).

