ifndef ROOT
	ROOT=$(shell pwd)
endif

all: targets

targets:
	cd src; ROOT=$(ROOT) make;
	cd test; ROOT=$(ROOT) make;

clean:	
	rm -rf erl_crash.dump *~
	cd src; ROOT=$(ROOT) make clean
	cd test; ROOT=$(ROOT) make clean


