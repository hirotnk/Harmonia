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

-module(hm_event_mgr).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-export([start_link/0,
         add_file_handler/0,
         add_handler/2,
         delete_file_handler/0,
         log/1,
         log/3
         ]).
-define(SERVER, ?MODULE).
-include("harmonia.hrl").

start_link() ->
    gen_event:start_link({local, ?SERVER}).

add_handler(Handler, Args) ->
    gen_event:add_handler(?SERVER, Handler, Args).

add_file_handler() ->
    gen_event:add_handler(?SERVER, hm_log_h_file, []).

delete_file_handler() ->
    gen_event:delete_handler(?SERVER, hm_log_h_file, []).

log(Msg) ->
    gen_event:notify(?SERVER, {log, Msg}).

log(Type, Format, DataList) ->
    {{Year,Month,Day}, {Hour,Minute,Second}} = erlang:localtime(),
    {_MegaSec, _Sec, Usec} = now(),
    Buf = io_lib:format(
        "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w.~6..0w :: " ++ "[~p:~p:~p:~p:~p]:~n" ++ Format,
        [Year, Month, Day, Hour, Minute, Second, Usec] ++ DataList
    ),
    case Type of 
        ?LOG_INFO    -> log("[INFO]    === " ++ Buf);
        ?LOG_WARNING -> log("[WARNING] === " ++ Buf);
        ?LOG_ERROR   -> log("[ERROR]   === " ++ Buf)
    end.

