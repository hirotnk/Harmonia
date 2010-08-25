-module(hm_name_sup).
-behaviour(supervisor).
-export([start/0, stop/0]).
-export([init/1]).

-include("harmonia.hrl").
-define(hm_name, hm_name_server).


start() -> supervisor:start_link({local, ?MODULE}, ?MODULE, {}).
stop() -> exit(whereis(?MODULE), kill).

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

