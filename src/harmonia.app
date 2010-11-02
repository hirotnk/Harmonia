{application, harmonia, 
 [{description, "Harmonia - Key/Value store engine with flexible range query support"},
  {vsn, "0.1"},
  {modules, [
    harmonia, hm_cache, hm_cache_mgr, hm_cli, hm_config, hm_config_if,
    hm_ds, hm_edge, hm_event_mgr, hm_log_h_file, hm_log_h_term, hm_misc,
    hm_name_server, hm_qp, hm_router, hm_stabilizer, hm_sup, hm_table
  ]},
  {registered, [
    hm_ds, hm_event_mgr, hm_name_server, hm_router, hm_stabilizer, 
    hm_sup, hm_table, harmonia, hm_cache_mgr, hm_config, hm_config_if
  ]},
  {applications, [kernel, stdlib]},
  {mod, {harmonia, []}},
  {start_phases, []},
  {env, [
      {node_type,   create_or_join},
      {name,        noname},
      {sname,       node_name},
      {root,        foo},
      {root_node,   foo@node},
      {logfile,     "harmonia_log"},
      {logfile_ext, ".txt"},
      {logdir,      "log/"}
  ]}
 ]}.

