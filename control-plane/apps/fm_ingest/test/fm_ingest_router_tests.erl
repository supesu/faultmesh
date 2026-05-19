-module(fm_ingest_router_tests).
-include_lib("eunit/include/eunit.hrl").

deterministic_test() ->
    A = fm_ingest_router:partition_for(<<"t1">>, <<"a1">>),
    B = fm_ingest_router:partition_for(<<"t1">>, <<"a1">>),
    ?assertEqual(A, B).

in_range_test() ->
    Count = fm_ingest_router:partition_count(),
    P = fm_ingest_router:partition_for(<<"tenant-X">>, <<"agent-Y">>),
    ?assert(P >= 0),
    ?assert(P < Count).

spread_test() ->
    Partitions = [fm_ingest_router:partition_for(<<"t">>, integer_to_binary(N))
                  || N <- lists:seq(1, 32)],
    ?assert(length(lists:usort(Partitions)) > 1).
