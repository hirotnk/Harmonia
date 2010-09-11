{application, harmonia, 
 [{description, "Harmonia - Key/Value store engine with flexible range query support"},
  {vsn, "0.1"},
  {modules, [
    harmoina, hm_ds, hm_misc, hm_name_server, hm_name_sup, hm_qp,
    hm_router, hm_stabilizer, hm_sup, hm_table
  ]},
  {registered, [
    harmonia, hm_ds, hm_router, hm_stabilizer, hm_sup, hm_table
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

