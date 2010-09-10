-module(hm_cli).
-export([
        cget/1,
        cstore/2,
        get/1,
        get/3,
        get_node_names/0,
        get_table_info/2,
        log_start/0,
        log_stop/0,
        make_table/3,
        store/2,
        store/3
        ]).

%% @spec(store(Key::atom()|string()|integer(), Value::any()) -> 
%%           {ok, Cnt::integer()} | 
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string}).
%%
%% @doc simple K/V store api
store(Key, Value) ->
    hm_ds:store(Key, Value).


%% @spec(store(Key::atom()|string()|integer(), Value::any()) ->
%%          {ok, Cnt::integer()} | 
%%          {partial, Cnt::integer()} |
%%          {ng, Msg::string}).
%%
%% @doc simple K/V store api with cache enabled
cstore(Key, Value) ->
    hm_ds:cstore(Key, Value).


%% @spec(store(DomainName::string(), TableName::string(), KVList::list()) -> 
%%           {ok, Cnt::integer()} |  
%%           {partial, Cnt::integer()} |
%%           {ng, Msg::string} |
%%           {error, Msg::string}).
%%
%% @doc store API for table format.
%%      this API stores index and data.
%%
%%      format of KVList::[{FldName::string(), Vaue::any()}]
%%      Note: table for this domain & table name, and field definition need to be
%%            made in advance.
store(DomainName, TableName, KVList) ->
    hm_ds:store(DomainName, TableName, KVList).

%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::any}|
%%                                              {error, nodata}).
%%
%% @doc simple K/V get api
get(Key) ->
    hm_ds:get(Key).


%% @spec(get(Key::atom()|string()|integer()) -> {ok, Result::any}|
%%                                              {error, nodata}).
%%
%% @doc simple K/V get api with cache enabled
cget(Key) ->
    hm_ds:cget(Key).

%% @spec(get(DomainName::string(), TableName::string(), Cond::string()) ->
%%           {ok, Result}|{error, Msg}).
%%
%% @doc get API for table format.
%%      - format of Cond::string
%%      - available relational operator::==|!=|<|>|<=|>=
%%      - available logical operator::and|or
%%      - parenthesis also available::(|)
%%      - name of fields and tables need to be stringnum(start with character)
get(DomainName, TableName, Cond) ->
    hm_ds:get(DomainName, TableName, Cond).


%% @spec(get_node_names() -> {ok, NameList::list()}).
%%
%% @doc get list of nodes participating Harmonia ring
get_node_names() ->
    gen_server:call({global, hm_name_server}, get_name_list).


%% @spec(make_table(DomainName::string(), TableName::string(), AttList::list()) ->
%%           {ok, {NodeList::list(), FailedList::list()}}).
%%
%% @doc make table for storing index info of this table
%%      returns list of nodes in which tables were created, 
%%      and list of nodes which table creation failed
%%      TODO: this return value make sense??
make_table(DomainName, TableName, AttList) ->
    hm_table:make_table(DomainName, TableName, AttList).


%% @spec(get_table_info(DomainName::string(), TableName::string()) ->
%%           {ok, Tid, AttList::list()} |
%%           {error, no_node_available} | {error, no_table}
%%
%% @doc returns Table Id to access it, and field attributes
%%      TODO: Tid should be known to users??
get_table_info(DomainName, TableName) ->
    hm_table:get_table_info(DomainName, TableName).

%% @spec(log_start() -> ok).
%%
%% @doc start logging for all nodes
log_start() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_start_in(NameList).

%% @spec(log_stop() -> ok).
%%
%% @doc stop logging for all nodes
log_stop() ->
    {ok, NameList} = gen_server:call({global, hm_name_server}, get_name_list),
    log_stop_in(NameList).

%% ----------------------------------------------------------------------------
%% Internal Functions
%% ----------------------------------------------------------------------------
log_start_in([]) -> ok;
log_start_in([{_Name, NodeName}|NodeList]) ->
    rpc:call(NodeName, hm_event_mgr, add_file_handler, []),
    log_start_in(NodeList).

log_stop_in([]) -> ok;
log_stop_in([{_Name, NodeName}|NameList]) ->
    rpc:call(NodeName, hm_event_mgr, delete_file_handler, []),
    log_stop_in(NameList).
