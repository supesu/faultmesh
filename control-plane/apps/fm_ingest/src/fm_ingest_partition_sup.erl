-module(fm_ingest_partition_sup).
-behaviour(supervisor).

-export([start_link/0, start_partition/2, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_partition(pid(), non_neg_integer()) -> supervisor:startchild_ret().
start_partition(HandlerPid, PartitionId) ->
    supervisor:start_child(?MODULE, [HandlerPid, PartitionId]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    Child = #{id => fm_ingest_partition,
              start => {fm_ingest_partition, start_link, []},
              restart => transient,
              type => worker},
    {ok, {SupFlags, [Child]}}.
