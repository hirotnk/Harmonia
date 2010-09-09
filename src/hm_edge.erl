-module(hm_edge).
-export([
        start/0,
        stop/0,
        stop/1
        ]).

start() ->
    case application:start(harmonia) of
        ok -> 
            io:fwrite("harmonia application started.~n");
        {error, Msg} ->
            io:fwrite("harmonia application start failed:~p~n", Msg)
    end.

% Note: This API needs to be executed on the node on which hm_name_server runs
%       because it has to be already connected to all nodes.
%
% this API takes a while to finish.
stop() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    stop_in(NameList).
stop_in([]) -> ok;
stop_in([{_Name, NodeName}|NameList]) ->
    ok = rpc:call(NodeName, application, stop, [harmonia]),
    ok = rpc:call(NodeName, init, stop, []),
    io:fwrite("harmonia application stopping.~n"),
    stop_in(NameList).

stop([RootNode]) -> rpc:call(RootNode, hm_edge, stop, []).

