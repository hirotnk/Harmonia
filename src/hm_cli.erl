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
    hm_event_mgr:add_file_handler().

log_stop() ->
    hm_event_mgr:delete_file_handler().

make_table(DomainName, TableName, AttList) ->
    hm_table:make_table(DomainName, TableName, AttList).
        
get_table_info(DomainName, TableName) ->
    hm_table:get_table_info(DomainName, TableName).

store(DomainName, TableName, KVList) ->
    hm_ds:store(DomainName, TableName, KVList).

get(DomainName, TableName, Cond) ->
    hm_ds:get(DomainName, TableName, Cond).

