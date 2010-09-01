% length of the digest of crypto:sha/1
-ifdef(debug).
-define(key_bit_length, 8).
-else.
-define(key_bit_length, 160).
-endif.

% registered name server id
-define(name_server, hm_name_server).

% Interval of stabilizer
-define(stabilize_interval, 3000).

% Interval of fixfingers
-define(fixfinger_interval, 3000).

% get timeout
-define(TIMEOUT_GET, 30000).

% Length of finger table
-define(max_finger, ?key_bit_length). 

% Length of successor list
-define(succ_list_len, (?key_bit_length bsr 1)). 

% max key value
-define(max_key_value, ((1 bsl ?key_bit_length) - 1)).

% logging info
-define(LOG_INFO, info).
-define(LOG_WARNING, warning).
-define(LOG_ERROR, error).

-define(log_parts (RegName, Data), ([node(), self(), ?MODULE, ?LINE, RegName] ++ Data)).
-define(info_p    (Fmt, RegName, Data), hm_event_mgr:log(?LOG_INFO,    Fmt, ?log_parts(RegName, Data))).
-define(warning_p (Fmt, RegName, Data), hm_event_mgr:log(?LOG_WARNING, Fmt, ?log_parts(RegName, Data))).
-define(error_p   (Fmt, RegName, Data), hm_event_mgr:log(?LOG_ERROR,   Fmt, ?log_parts(RegName, Data))).

% process prefix
-define(PROCESS_PREFIX, "hm_router_").

-record(state, {node_name, node_vector, predecessor=nil, finger=[], succlist=[], current_fix=0}).

