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
CFLG = -W +warn_unused_vars +warn_unused_import +debug_info 
ERL = erl
GEN = beam
ERLC_EMULATOR = erl -boot start_clean
PATH= .:$(ERLHOME)/bin:/bin:/usr/bin:/usr/local/bin
SOURCE = harmonia.erl \
	hm_sup.erl \
	hm_misc.erl \
	hm_router.erl \
	hm_ds.erl \
	hm_table.erl \
	hm_stabilizer.erl \
	hm_config.erl \
	hm_name_sup.erl \
	hm_name_server.erl \
	hm_event_mgr.erl \
	hm_log_h_file.erl \
	hm_log_h_term.erl \
	hm_qp.erl \
	hm_startup.erl \
	hm_cli.erl

TARGETS = $(SOURCE:%.erl=$(EBIN)/%.beam)
CODE = $(SOURCE:%.erl=$(EBIN)/%.beam) 

$(EBIN)/%.beam: %.erl
	/usr/bin/erlc  $(EDBG) $(CFLG) -v -b beam -o $(EBIN) $(EFLAGS) $<

all: $(TARGETS)
	cp harmonia.app ../ebin/harmonia.app

debug: $(TARGETS)
	/usr/bin/erlc $(EDBG) $(CFLG) -v -b beam -o $(EBIN) $(EFLAGS) $<
   
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
EFLAGS = -I../include 

#######################################################################
# Do not edit below this line                                         #
#######################################################################
#Include following generic make script
#include $(TOOLSHOME)/erlang


