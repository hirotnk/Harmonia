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
PA = /usr/local/lib/erlang/lib/eunit-2.1.5
ERL = erl
GEN = beam
ERLC_EMULATOR = erl -boot start_clean
PATH= .:$(ERLHOME)/bin:/bin:/usr/bin:/usr/local/bin
SOURCE = hm_cli_test.erl \
	hm_cache_test.erl \
	hm_test.erl

TARGETS = $(SOURCE:%.erl=$(EBIN)/%.beam)
CODE = $(SOURCE:%.erl=$(EBIN)/%.beam) 

$(EBIN)/%.beam: %.erl
	/usr/bin/erlc -pa $(PA) $(EDBG) $(CFLG) -v -o $(EBIN) -b beam $(EFLAGS) $<

all: $(TARGETS)

debug: $(TARGETS)
	/usr/bin/erlc -pa $(PA) $(EDBG) $(CFLG) -v -o $(EBIN) -b beam $(EFLAGS) $<
   
clean:
	\rm -f $(CODE) 


#######################################################################
# Template of for compiling erlang files                              #
# The environment variable  $TOOLSHOME home has to be set to where    #  
# the generic make script is installed (erlang).                      #
#######################################################################
# code to compile

#Where include files are stored ".hrl"
EFLAGS = -I../include:/usr/local/lib/erlang/lib

#######################################################################
# Do not edit below this line                                         #
#######################################################################
#Include following generic make script
#include $(TOOLSHOME)/erlang


