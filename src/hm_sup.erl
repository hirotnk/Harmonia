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
%%%-------------------------------------------------------------------
%%% @author Yoshihiro TANAKA <hirotnkg@gmail.com>
%%% @copyright (C) 2010, Yoshihiro TANAKA
%%% @doc
%%%  Harmonia supervisor
%%% @end
%%% Created :  2 Oct 2010 by Yoshihiro <hirotnkg@gmail.com>
%%%-------------------------------------------------------------------
-module(hm_sup).
-author('Yoshihiro TANAKA <hirotnkg@gmail.com>').
-behaviour(supervisor).
%% API
-export([
        create/2,
        join/3,
        start_link/1,
        stop/1
        ]).
%% Supervisor callbacks
-export([init/1]).

-include("harmonia.hrl").
-define(connect_try, 5).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
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
                io:fwrite("ERR:~p\n", [Any])
        end,
    {ok, {Name, NodeName}} = hm_name_server:register({Name, NodeName}),
    case hm_log_h_file:add() of
        ok -> ok;
        Err -> io:fwrite("ERR: log file handler add:[~p]~n", [Err])
    end,
    io:fwrite("start Pid:[~p]~n", [Pid]),
    {ok, Pid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init({{create, Name}          = Arg, Env}) -> create_children(create, Arg, Name, Env);
init({{join, Name, _RootName} = Arg, Env}) -> create_children(join,   Arg, Name, Env).

%%%===================================================================
%%% Internal functions
%%%===================================================================
create(Name, Env)         -> supervisor:start_link({global, Name}, ?MODULE, {{create, Name}, Env}).

join(Name, RootName, Env) -> supervisor:start_link({global, Name}, ?MODULE, {{join, Name, RootName}, Env}).

create_children(Type, Arg, Name, Env) -> 
    ServerListTmp = 
        [
            child(hm_config,     hm_config,     [Env], worker),
            child(hm_config_if,  hm_config_if,  [{Name, Env}], worker),
            child(hm_event_mgr,  gen_event,     [{local, hm_event_mgr}], worker),
            child(hm_router,     hm_router,     [Arg],  worker),
            child(hm_stabilizer, hm_stabilizer, [Name], worker),
            child(hm_ds,         hm_ds,         [Name], worker),
            child(hm_table,      hm_table,      [Name], worker),
            child(hm_cache_mgr,  hm_cache_mgr,  [Name], worker)
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

connect_node(Node) ->
    connect_node_in(Node, ?connect_try).

connect_node_in(_Node, 0) -> {error, fail_to_connect};
connect_node_in(Node, Cnt) ->
    case net_kernel:connect_node(Node) of 
        true    -> 
            timer:sleep(1000), 
            global:sync(),
            ok;
        false   -> 
            timer:sleep(1000), 
            connect_node_in(Node, Cnt - 1);
        ignored -> {error, localnode_not_alive}
    end.
%% testing branch..
%% start_hm_name() -> 
%%     supervisor:start_child(?NAME_SUP, child(?NAME_SUP,
%%                                             ?NAME_SUP,
%%                                             [], supervisor)).
%% stop_hm_name(RegName) -> 
%%     supervisor:terminate_child(name(RegName), ?name_server).
