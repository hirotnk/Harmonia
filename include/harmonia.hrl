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

% Length of finger table
-define(max_finger, ?key_bit_length). 

% Length of successor list
-define(succ_list_len, (?key_bit_length bsr 1)). 

% max key value
-define(max_key_value, ((1 bsl ?key_bit_length) - 1)).

% logging info
-define(debug_p  (Fmt, RegName, Data), error_logger:info_msg("[~p:~p:~p:~p:~p:~p]:~n" ++ Fmt, [node(), ?MACHINE, ?FILE, ?MODULE, ?LINE, RegName] ++ Data)).

% process prefix
-define(PROCESS_PREFIX, "harmonia_").

-record(state, {node_name, node_vector, predecessor=nil, finger=[], succlist=[], current_fix=0}).

