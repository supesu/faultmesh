-module(fm_ingest_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => rest_for_one, intensity => 5, period => 10},
    Children = lists:filter(fun(undefined) -> false; (_) -> true end, [
        spiffe_child(),
        #{id => fm_ingest_wal_sup,
          start => {fm_ingest_wal_sup, start_link, []},
          type => supervisor},
        #{id => fm_ingest_sink,
          start => {fm_ingest_sink, start_link, []},
          type => worker},
        #{id => fm_ingest_partition_sup,
          start => {fm_ingest_partition_sup, start_link, []},
          type => supervisor}
    ]),
    {ok, {SupFlags, Children}}.

spiffe_child() ->
    case application:get_env(fm_ingest, spire_enabled, false) of
        true ->
            #{id => fm_ingest_spiffe,
              start => {fm_ingest_spiffe, start_link, []},
              type => worker};
        false ->
            undefined
    end.
