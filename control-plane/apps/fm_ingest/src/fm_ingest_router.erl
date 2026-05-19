-module(fm_ingest_router).

-export([partition_count/0, partition_for/2]).

-define(DEFAULT_PARTITIONS, 64).

-spec partition_count() -> pos_integer().
partition_count() ->
    application:get_env(fm_ingest, partition_count, ?DEFAULT_PARTITIONS).

-spec partition_for(binary(), binary()) -> non_neg_integer().
partition_for(Tenant, Agent) when is_binary(Tenant), is_binary(Agent) ->
    erlang:phash2({Tenant, Agent}, partition_count()).
