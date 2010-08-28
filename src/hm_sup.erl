-module(hm_sup).
-behaviour(supervisor).
-export([start_link/1, create/2, join/3, stop/1]).
-export([init/1]).

-include("harmonia.hrl").

start_link(Env) -> 
    case proplists:get_value(node_type, Env) of
        create -> 
            create(proplists:get_value(name, Env), Env);
        join ->
            join(proplists:get_value(name, Env),
                 proplists:get_value(root, Env), Env)
    end.

create(Name, Env)         -> supervisor:start_link({global, Name}, ?MODULE, {{create, Name}, Env}).
join(Name, RootName, Env) -> supervisor:start_link({global, Name}, ?MODULE, {{join, Name, RootName}, Env}).

init({{create, Name}          = Arg, Env}) -> create_children(Arg, Name, Env);
init({{join, Name, _RootName} = Arg, Env}) -> create_children(Arg, Name, Env).

create_children(Arg, Name, Env) -> 
    Config     = child(hm_config,     Env, worker),
    Router     = child(hm_router,     Arg,  worker),
    Stabilizer = child(hm_stabilizer, Name, worker),
    DataStore  = child(hm_ds,         Name, worker),
    Table      = child(hm_table,      Name, worker),

    ServerList = [
                  Config,
                  Router,
                  Stabilizer,
                  DataStore,
                  Table
                  ],
    {ok, { {one_for_one, 5, 2000}, ServerList } }.

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

stop(Name) -> exit(global:whereis_name(Name), kill).

name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).

%% start_hm_name(RegName) -> 
%%     supervisor:start_child(name(RegName), child(?name_server,
%%                                                 ?name_server,
%%                                                 worker)).
%% stop_hm_name(RegName) -> 
%%     supervisor:terminate_child(name(RegName), ?name_server).

