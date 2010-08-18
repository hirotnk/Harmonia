-module(harmonia_sup).
-behaviour(supervisor).
-export([create/1, join/2, stop/1]).
-export([init/1]).

create(Name) -> supervisor:start_link({local, Name}, ?MODULE, {create, Name}).
join(Name, RootName) -> supervisor:start_link({local, Name}, ?MODULE, {join, Name, RootName}).
stop(Name) -> exit(whereis(Name), kill).

init({create, Name} = Arg) ->
    %%%%% TODO: later make this part to application:get_env
    error_logger:tty(false),
    error_logger:logfile({open, "./" ++ atom_to_list(Name) ++ "log.txt"}),
    %%%%
    child_spec(Arg, Name);

init({join, Name, RootName} = Arg) ->
    %%%%% TODO: later make this part to application:get_env
    error_logger:tty(false),
    error_logger:logfile({open, "./" ++ atom_to_list(Name) ++ "log.txt"}),
    %%%%
    child_spec(Arg, Name).

child_spec(Arg, Name) ->
    {ok, { {one_for_one, 5, 2000}, % restart taple
              [                    % child spec list
               child(harmonia,            Arg, worker),
               child(harmonia_stabilizer, Name, worker),
               child(harmonia_ds, Name, worker),
               child(harmonia_table, Name, worker)
              ] } }.


child(Module, Arg, Type) ->
  {Module,         % Name
      {            % Start Function
          Module,          % Module
          start_link,      % Function
          [Arg]            % Arg
      }, 
      permanent,   % restart type 
      brutal_kill, % Shutdown time
      Type,        % Process type
      [Module]     % Modules
  }.


%start_hm(Name) -> 
%    supervisor:start_child(Name, child(harmonia_sup, Name, supervisor)).
%stop_hm(Name) -> 
%    supervisor:terminate_child(Name, Name).
