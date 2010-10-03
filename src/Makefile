#######################################################################
# Generic make script for compiling erlang code                       #
# The environment variable $ERLHOME has to be set to where erlang/OTP #
# is installed                                                        #
# Compiles the code into a ebin dir. relative to the source dir.      #
# (../ebin)                                                           #
####################################################################### 
#Compiles the code into a ebin dir. relative to the source dir. 
EBIN = ../ebin
EDBG = -Ddebug
CFLG = -W +warn_unused_vars +warn_unused_import +debug_info +native +"{hipe, [o3]}"

ifeq ($(CFG),nolog) 
NOLOG = -Dnolog
else 
NOLOG = 
endif 
 
ERL = erl
PA = /usr/local/lib/erlang/lib/eunit-2.1.5/ebin
GEN = beam
ERLC_EMULATOR = erl -boot start_clean
PATH= .:$(ERLHOME)/bin:/bin:/usr/bin:
SOURCE = harmonia.erl \
	hm_cache.erl \
	hm_cache_mgr.erl \
	hm_cli.erl \
	hm_config.erl \
	hm_config_if.erl \
	hm_ds.erl \
	hm_edge.erl \
	hm_event_mgr.erl \
	hm_log_h_file.erl \
	hm_log_h_term.erl \
	hm_misc.erl \
	hm_name_server.erl \
	hm_qp.erl \
	hm_router.erl \
	hm_stabilizer.erl \
	hm_sup.erl \
	hm_table.erl

TARGETS = $(SOURCE:%.erl=$(EBIN)/%.beam)
CODE = $(SOURCE:%.erl=$(EBIN)/%.beam) 

$(EBIN)/%.beam: %.erl
	/usr/bin/erlc -pa $(PA) $(NOLOG) $(EDBG) $(CFLG) -v -b beam -o $(EBIN) $(EFLAGS) $<

all: $(TARGETS)
	cp harmonia.app ../ebin/harmonia.app

clean:
	\rm -f $(CODE) 
	\rm -f ../ebin/harmonia.app


#######################################################################
# Template of for compiling erlang files                              #
# The environment variable  $TOOLSHOME home has to be set to where    #  
# the generic make script is installed (erlang).                      #
#######################################################################
# code to compile

#Where include files are stored ".hrl"
EFLAGS = -I../include -I/usr/local/lib/erlang/lib/stdlib-1.16.5/


#######################################################################
# Do not edit below this line                                         #
#######################################################################
#Include following generic make script
#include $(TOOLSHOME)/erlang


