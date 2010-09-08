-module(hm_cache).
-behaviour(gen_server).
-vsn('0.1').

-export([
        get_cache/1,
        start_link/1, 
        stop/0,
        store_cache/2
    ]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2]).

-include_lib("include/ms_transform.hrl").
-include("harmonia.hrl").

-define(cache_timeout, (60*60)).  % default 1 hour
-define(cache_limit_size, 10000). % default 10000 recs
-define(ets_cache_table, ets_cache_table).


start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Env, []).

stop() ->
    gen_server:cast(?MODULE, stop).

store_cache(Key, Value) ->
    gen_server:call(?MODULE, {store, Key, Value}).

get_cache(Key) ->
    case ets:lookup(?ets_cache_table, Key) of
            %[{{'$1','$2','_','_'},[{'=:=','$1',Key}],['$2']}]) of
            %[{{'$1','$2','$3','_'},[{'=:=','$1',Key}],['$2']}]) of
                   %[{{'$1','$2','$3','_'},[{'=:=','$1',Key}],[{{'$2','$3'}}]}]) of
        [] -> none;
        [{Key, Value, Cnt, _}] -> 
            % {"key10000",{10000,"key10000"},0,1283991518}
           {MegaSecs, Secs, _Microsecs} = now(),
           ets:update_element(
               ?ets_cache_table, 
               Key, 
               [{3,Cnt+1},{4, MegaSecs*1000000 + Secs + ?cache_timeout}]
           ),
           {ok, {Value, Cnt}}
    end.

terminate(Reason, State) -> 
    %?info_p("terminate:Reason:[~p] State:[~p]~n", none, [Reason, State]),
    ok.

init(Env) ->
    Limit = 
        case proplists:get_value(cache_limit_size, Env) of 
            undefined -> ?cache_limit_size;
            Lim -> Lim
        end,
    Timeout = 
        case proplists:get_value(cache_timeout, Env) of
            undefined -> ?cache_timeout;
            Tout -> Tout
        end,
    ets:new(?ets_cache_table, [named_table, public]),
    {ok, {Limit, Timeout}}. 

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_call(info, _From, State) ->
    {reply, State, State};

handle_call({store, Key, Value}, _From, State) ->
    {MegaSecs, Secs, _Microsecs} = now(),
    Ret = (
            catch 
                ets:insert(
                    ?ets_cache_table, 
                    {Key, Value, 0, MegaSecs*1000000 + Secs + ?cache_timeout}
                )
          ),
    %case Ret of
    %   true -> ok;
    %   Msg ->
    %       ?error_p("failed to insert cache Key:[~p] Value:[~p] Msg:[~p] ~n", none, [Key, Value, Msg])
    %end,
    {reply, ok, State};
    %{noreply, State}.

handle_call({get, Key}, _From, State) ->
    Res = 
    case ets:select(?ets_cache_table, 
            [{{'$1','$2','_','_'},[{'=:=','$1',Key}],['$2']}]) of
            %[{{'$1','$2','$3','_'},[{'=:=','$1',Key}],['$2']}]) of
                   %[{{'$1','$2','$3','_'},[{'=:=','$1',Key}],[{{'$2','$3'}}]}]) of
        [] -> none;
        [Value] -> Value
        % [{Value, Cnt}] -> 
        %     {MegaSecs, Secs, _Microsecs} = now(),
        %     ets:update_element(
        %         ?ets_cache_table, 
        %         Key, 
        %         [{3,Cnt+1},{4, MegaSecs*1000000 + Secs + ?cache_timeout}]
        %     ),
        %     Value % return value
    end,
    {reply, Res, State}.
