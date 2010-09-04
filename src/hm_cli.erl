-module(hm_cli).
-export([
        get/3,
        get_table_info/2,
        log_start/0,
        log_stop/0,
        make_table/3,
        store/3
        ]).

log_start() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_start_in(NameList).

log_start_in([]) -> ok;
log_start_in([Name|NodeList]) ->
    {ok, {sname, NodeName}} = gen_server:call({global, hm_config_if:name(Name)}, {get, sname}),
    rpc:call(NodeName, hm_event_mgr, add_file_handler, []),
    log_start_in(NodeList).

log_stop() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_stop_in(NameList).

log_stop_in([]) -> ok;
log_stop_in([Name|NameList]) ->
    {ok, {sname, NodeName}} = gen_server:call({global, hm_config_if:name(Name)}, {get, sname}),
    rpc:call(NodeName, hm_event_mgr, delete_file_handler, []),
    log_stop_in(NameList).

make_table(DomainName, TableName, AttList) ->
    {ok, NodeName} = get_node_name(),
    rpc:call(NodeName, hm_table, make_table, [DomainName, TableName, AttList]).

get_table_info(DomainName, TableName) ->
    {ok, NodeName} = get_node_name(),
    rpc:call(NodeName, hm_table, get_table_info, [DomainName, TableName]).

store(DomainName, TableName, KVList) ->
    {ok, NodeName} = get_node_name(),
    rpc:call(NodeName, hm_ds, store, [DomainName, TableName, KVList]).

get(DomainName, TableName, Cond) ->
    {ok, NodeName} = get_node_name(),
    rpc:call(NodeName, hm_ds, get, [DomainName, TableName, Cond]).

get_node_name() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    Name = lists:nth(random:uniform(length(NameList)), NameList),
    {ok, {sname, NodeName}} = gen_server:call({global, hm_config_if:name(Name)}, {get, sname}),
    {ok, NodeName}.

