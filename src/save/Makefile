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
ERL = erl
GEN = beam
ERLC_EMULATOR = erl -boot start_clean
PATH= .:$(ERLHOME)/bin:/bin:/usr/bin:/usr/local/bin
SOURCE = harmonia_ds.erl \
	harmonia.erl \
	harmonia_stabilizer.erl \
	harmonia_sup.erl \
	hm_misc.erl

TARGETS = $(SOURCE:%.erl=$(EBIN)/%.beam)
CODE = $(SOURCE:%.erl=$(EBIN)/%.beam) 

$(EBIN)/%.beam: %.erl
	/usr/bin/erlc  $(EDBG) -W -b beam -o $(EBIN) $(EFLAGS) $<

all: $(TARGETS)

debug: $(TARGETS)
	/usr/bin/erlc -Ddebug -W -b beam -o $(EBIN) $(EFLAGS) $<
   
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


