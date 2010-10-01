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

-module(hm_edge).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-export([
        start/0,
        stop/0,
        stop/1
        ]).

start() ->
    {ok, [[Nodename]]} = init:get_argument(sname),
    error_logger:logfile({open, "SYS_INFO_" ++ Nodename ++ ".log"}),
    case application:start(harmonia) of
        ok -> ok;
        {error, _Msg} -> init:stop(1)
    end.

stop() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    stop_in(NameList).
stop_in([]) -> ok;
stop_in([{_Name, NodeName}|NameList]) ->
    case rpc:call(NodeName, application, stop, [harmonia]) of
        ok ->
            case rpc:call(NodeName, init, stop, []) of
                ok -> 
                    io:fwrite("Stopped the node [~p].~n", [NodeName]);
                {badrpc, Reason} ->
                    io:fwrite("Failed to stop the node [~p].~n", [Reason])
            end;
        {badrpc, Reason} ->
            io:fwrite("Failed to stop harmonia on the node [~p].~n", [Reason])
    end,
    stop_in(NameList).

stop([RootNode]) -> rpc:call(RootNode, hm_edge, stop, []).

