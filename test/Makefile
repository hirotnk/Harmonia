#######################################################################
# Generic make script for compiling erlang code                       #
# The environment variable $ERLHOME has to be set to where erlang/OTP #
# is installed                                                        #
# Compiles the code into a ebin dir. relative to the source dir.      #
# (../ebin)                                                           #
####################################################################### 
#Compiles the code into a ebin dir. relative to the source dir. 
EDBG = -Ddebug
CFLG = -W +warn_unused_vars +warn_unused_import +debug_info 
ERL = erl
GEN = beam
ERLC_EMULATOR = erl -boot start_clean
PATH= .:$(ERLHOME)/bin:/bin:/usr/bin:/usr/local/bin
PA = /usr/local/lib/erlang/lib/eunit-2.1.5/
SOURCE = hm_qp_test.erl \
	hm_cli_test.erl

TARGETS = $(SOURCE:%.erl=./%.beam)
CODE = $(SOURCE:%.erl=./%.beam) 

./%.beam: %.erl
	/usr/bin/erlc -pa $(PA) $(EDBG) $(CFLG) -v -b beam $(EFLAGS) $<

all: $(TARGETS)

debug: $(TARGETS)
	/usr/bin/erlc -pa $(PA) $(EDBG) $(CFLG) -v -b beam $(EFLAGS) $<
   
clean:
	\rm -f $(CODE) 


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


