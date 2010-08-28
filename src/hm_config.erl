-module(hm_config).
-behaviour(gen_server).
-vsn('0.1').

-export([start_link/1, stop/0, name/1, get/1]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include("harmonia.hrl").

start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Env, []).

stop() ->
    gen_server:cast(?MODULE, stop).

terminate(_Reason, _State) -> ok.

init(Env) -> {ok, Env}. 

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call({get, Key}, _From, Env) ->
    Reply = 
        case proplist:get_value(Key, Env) of 
            Value -> {ok, {Key, Value}};
            undefined -> {error, key_undefined}
        end,
    {reply, Reply, Env}.

get(Key) ->
    {ok, Value } = gen_server:call({local, ?MODULE}, {get, Key}).
    
name(RegName) -> list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(RegName)).
