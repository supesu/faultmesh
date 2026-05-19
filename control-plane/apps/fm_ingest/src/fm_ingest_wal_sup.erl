-module(fm_ingest_wal_sup).
-behaviour(supervisor).

-export([start_link/0, start_wal/1, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_wal(non_neg_integer()) -> supervisor:startchild_ret().
start_wal(PartId) ->
    supervisor:start_child(?MODULE, [PartId]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    Child = #{id => fm_ingest_wal,
              start => {fm_ingest_wal, start_link, []},
              restart => transient,
              type => worker},
    {ok, {SupFlags, [Child]}}.
