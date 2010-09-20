ifndef ROOT
	ROOT=$(shell pwd)
endif

all: targets

nolog:	nologs

targets:
	cd src; ROOT=$(ROOT) make;
	cd test; ROOT=$(ROOT) make;

nologs:
	cd src; ROOT=$(ROOT) make CFG="nolog";
	cd test; ROOT=$(ROOT) make;

clean:	
	rm -rf erl_crash.dump *~
	cd src; ROOT=$(ROOT) make clean
	cd test; ROOT=$(ROOT) make clean


