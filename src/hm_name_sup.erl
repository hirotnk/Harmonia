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

-module(hm_name_sup).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(supervisor).
-export([start/0, stop/0]).
-export([init/1]).

-include("harmonia.hrl").
-define(hm_name, hm_name_server).


start() -> supervisor:start_link({global, ?MODULE}, ?MODULE, {}).
start_shell() -> supervisor:start({global, ?MODULE}, ?MODULE, {}).
stop() -> exit(global:whereis_name(?MODULE), kill).

init(_) -> 
    {ok, { {one_for_one, 5, 2000}, % restart taple
            [child(?hm_name, worker)]
          } }.

child(Module, Type) ->
  {Module,         % Name
      {            % Start Function
          Module,          % Module
          start_link,      % Function
          []            % Arg
      }, 
      permanent,   % restart type 
      brutal_kill, % Shutdown time
      Type,        % Process type
      [Module]     % Modules
  }.

