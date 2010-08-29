-module(hm_sup).
-behaviour(supervisor).
-export([
        create/2, 
        join/3, 
        start_link/1, 
        stop/1
        ]).
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
    ServerList = 
        [
            child(hm_config,     hm_config,     Env, worker),
            child(hm_event_mgr,  gen_event,     {local, hm_event_mgr}, worker),
            child(hm_router,     hm_router,     Arg,  worker),
            child(hm_stabilizer, hm_stabilizer, Name, worker),
            child(hm_ds,         hm_ds,         Name, worker),
            child(hm_table,      hm_table,      Name, worker)
        ],
    {ok, { {one_for_one, 5, 2000}, ServerList } }.

child(Name, Module, Arg, Type) ->
  {Name,           % Name
      {            % Start Function
          Module,          % Module
          start_link,      % Function
          [Arg]            % Arg
      }, 
      permanent,   % restart type 
      1000,        % Shutdown time
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

