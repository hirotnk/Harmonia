% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(hm_test).
-export([
        dbgadd/1,
        dbgall/0,
        dbgdel/1
        ]).

dbgall() ->
    dbg:tracer(),
    dbg:p(all, call),
    dbg:tpl(test,[]),
    dbg:tpl(test,x),
    dbg:tpl(tp,[]),
    dbg:tpl(tp,x),
    dbg:tpl(hm_cache,[]),
    dbg:tpl(hm_cache,x),
    dbg:tpl(hm_router,[]),
    dbg:tpl(hm_router,x),
    dbg:tpl(harmonia,[]),
    dbg:tpl(harmonia,x),
    dbg:tpl(hm_misc,[]),
    dbg:tpl(hm_misc,x),
    dbg:tpl(hm_ds,[]),
    dbg:tpl(hm_ds,x),
    dbg:tpl(hm_sup,[]),
    dbg:tpl(hm_sup,x),
    dbg:tpl(hm_name_server,[]),
    dbg:tpl(hm_name_server,x),
    dbg:tpl(hm_log_h_file,[]),
    dbg:tpl(hm_log_h_file,x),
    dbg:tpl(hm_event_mgr,[]),
    dbg:tpl(hm_event_mgr,x),
    dbg:tpl(hm_event,[]),
    dbg:tpl(hm_event,x),
    application:start(sasl).

dbgadd(Mod) ->
    dbg:tracer(),
    dbg:p(Mod, call),
    dbg:tpl(Mod,[]),
    dbg:tpl(Mod,x).

dbgdel(Mod) ->
    dbg:ctpl(Mod).

