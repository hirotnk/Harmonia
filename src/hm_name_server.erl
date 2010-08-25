-module(hm_name_server).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/0, stop/0, name/1]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

stop() ->
    gen_server:cast(?MODULE, stop).

terminate(_Reason, _State) -> ok.

init(_) -> {ok, []}. 

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call(get_name_list, _From, State) ->
    {reply, {ok, State}, State};

handle_call({register_name, RegName}, _From, State) ->
    {reply, {ok, RegName}, [RegName|State]}.

name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).
