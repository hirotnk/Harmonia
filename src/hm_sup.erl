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
-define(connect_try, 3).
%% -define(NAME_SUP, hm_name_server_sup)

start_link(Env) -> 
    Name = proplists:get_value(name, Env),
    Root = proplists:get_value(root, Env),
    NodeName = proplists:get_value(sname, Env),
    RootNode = proplists:get_value(root_node, Env),
    io:format("~p~n",[Env]),
    {ok, Pid} = 
        case proplists:get_value(node_type, Env) of
            create -> 
                create(Name, Env);
            join ->
                ok = connect_node(RootNode),
                join(Name, Root , Env);
            Any ->
                io:format("ERR:~p\n", [Any])
        end,
    {ok, Name} = gen_server:call({global, ?name_server}, {register_name, Name}),
    {ok, Pid}.

create(Name, Env)         -> supervisor:start_link({global, Name}, ?MODULE, {{create, Name}, Env}).
join(Name, RootName, Env) -> supervisor:start_link({global, Name}, ?MODULE, {{join, Name, RootName}, Env}).

init({{create, Name}          = Arg, Env}) -> create_children(create, Arg, Name, Env);
init({{join, Name, _RootName} = Arg, Env}) -> create_children(join,   Arg, Name, Env).

create_children(Type, Arg, Name, Env) -> 
    ServerListTmp = 
        [
            child(hm_config,     hm_config,     [Env], worker),
            child(hm_config_if,  hm_config_if,  [{Name, Env}], worker),
            child(hm_event_mgr,  gen_event,     [{local, hm_event_mgr}], worker),
            child(hm_router,     hm_router,     [Arg],  worker),
            child(hm_stabilizer, hm_stabilizer, [Name], worker),
            child(hm_ds,         hm_ds,         [Name], worker),
            child(hm_table,      hm_table,      [Name], worker)
        ],
    ServerList = 
        case Type =:= create of 
            true ->
                [child(?name_server, ?name_server, [], worker)] ++
                ServerListTmp;
            false -> 
                ServerListTmp
        end,
    {ok, { {one_for_one, 5, 2000}, ServerList } }.

child(Name, Module, Arg, Type) ->
  {Name,           % Name
      {            % Start Function
          Module,          % Module
          start_link,      % Function
          Arg              % Arg
      }, 
      permanent,   % restart type 
      1000,        % Shutdown time
      Type,        % Process type
      [Module]     % Modules
  }.

stop(Name) -> exit(global:whereis_name(Name), kill).

% name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).

connect_node(Node) ->
    connect_node_in(Node, ?connect_try).

connect_node_in(_Node, 0) -> {error, fail_to_connect};
connect_node_in(Node, Cnt) ->
    case net_kernel:connect_node(Node) of 
        true    -> timer:sleep(500), ok;
        false   -> connect_node_in(Node, Cnt - 1);
        ignored -> {error, localnode_not_alive}
    end.

%% start_hm_name() -> 
%%     supervisor:start_child(?NAME_SUP, child(?NAME_SUP,
%%                                             ?NAME_SUP,
%%                                             [], supervisor)).
%% stop_hm_name(RegName) -> 
%%     supervisor:terminate_child(name(RegName), ?name_server).

