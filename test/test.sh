#!/bin/bash

/usr/local/bin/erlc ./hm_qp_test.erl
/usr/local/bin/erl -pa ../ebin -s hm_qp_test test -s init stop
