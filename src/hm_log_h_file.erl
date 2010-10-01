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

-module(hm_log_h_file).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_event).
-export([
        add/0,
        delete/0
    ]).
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, code_change/3, terminate/2]).

-include("harmonia.hrl").

add() ->
    {ok, {_, Name}}    = hm_config:get(name),
    {ok, {_, Logfile}} = hm_config:get(logfile),
    {ok, {_, Ext}}     = hm_config:get(logfile_ext),
    {ok, {_, Logdir}}  = hm_config:get(logdir),
    
    FileName = Logdir ++ Logfile ++ "_" ++ atom_to_list(Name) ++ Ext,
    io:format("~p~n", [FileName]),
    hm_event_mgr:add_handler(?MODULE, list_to_atom(FileName)).

delete() ->
    hm_event_mgr:delete_handler(?MODULE, []).

init(Filename) ->
    {ok, Fd} = file:open(Filename, [write]), % truncate if exits
    {ok, {Fd, Filename}}.

handle_event({log, Msg}, {Fd, _Filename}=State) ->
    io:format(Fd, "~s", [Msg]),
    {ok, State}.
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.
handle_info(_Info, State) ->
    {ok, State}.
terminate(_Reason, {Fd, File}) ->
    io:format("~p:closing ~p~n", [?MODULE, File]),
    file:close(Fd),
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


