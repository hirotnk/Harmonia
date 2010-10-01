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

-module(hm_config_if).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(gen_server).
-vsn('0.1').

-export([
        get/1,
        name/1, 
        start_link/1, 
        stop/0 
        ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").

start_link({Name, Env}) ->
    gen_server:start_link({global, name(Name)}, ?MODULE, Env, []).

stop() ->
    gen_server:cast(?MODULE, stop).

terminate(Reason, State) -> 
    ?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    ok.

init(Env) -> {ok, Env}. 

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({get, Key}, _From, Env) ->
    Reply = 
        case proplists:get_value(Key, Env) of 
            undefined -> {error, key_undefined};
            Value -> {ok, {Key, Value}}
        end,
    {reply, Reply, Env}.

get(Key) ->
    {ok, _Value} = gen_server:call(?MODULE, {get, Key}).
    
name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).
