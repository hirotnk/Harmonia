-module(harmonia_sup).
-behaviour(supervisor).
-export([create/1, join/2, stop/1, start_hm_name/1, stop_hm_name/1]).
-export([init/1]).

-include("harmonia.hrl").


create(Name) -> supervisor:start_link({local, Name}, ?MODULE, {create, Name}).
join(Name, RootName) -> supervisor:start_link({local, Name}, ?MODULE, {join, Name, RootName}).
stop(Name) -> exit(whereis(Name), kill).

init({create, Name} = Arg) ->
    create_children(Arg, Name);
init({join, Name, _RootName} = Arg) ->
    create_children(Arg, Name).

create_children(Arg, Name) ->
    child_spec(child_spec_list(Arg, Name)).

child_spec(ChildSpecList) ->
    {ok, { {one_for_one, 5, 2000}, % restart taple
            ChildSpecList
          } }.

child_spec_list(Arg, Name) ->
    [                    % child spec list
        child(harmonia, Arg, worker),
        child(harmonia_stabilizer, Name, worker),
        child(harmonia_ds, Name, worker),
        child(harmonia_table, Name, worker)
    ]. 

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


start_hm_name(RegName) -> 
    supervisor:start_child(name(RegName), child(?name_server,
                                                ?name_server,
                                                worker)).
stop_hm_name(RegName) -> 
    supervisor:terminate_child(name(RegName), ?name_server).

name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).
