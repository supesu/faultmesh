.PHONY: all compile release tar test dialyzer xref clean shell

all: compile

compile:
	rebar3 compile

release:
	rebar3 as prod release

tar:
	rebar3 as prod tar

test:
	rebar3 eunit

dialyzer:
	rebar3 dialyzer

xref:
	rebar3 xref

shell:
	rebar3 shell

clean:
	rebar3 clean
	rm -rf _build
