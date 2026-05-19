-module(fm_common_peer_tests).
-include_lib("eunit/include/eunit.hrl").

parse_happy_path_test() ->
    Id = <<"spiffe://faultmesh.local/ns/tenant-a/agent/host-7">>,
    ?assertEqual({ok, <<"faultmesh.local">>, <<"tenant-a">>, <<"host-7">>},
                 fm_common_peer:parse_spiffe_id(Id)).

parse_with_extra_path_segments_test() ->
    Id = <<"spiffe://td/ns/T/agent/A/extra">>,
    ?assertEqual({ok, <<"td">>, <<"T">>, <<"A">>},
                 fm_common_peer:parse_spiffe_id(Id)).

parse_missing_ns_test() ->
    ?assertMatch({error, _},
                 fm_common_peer:parse_spiffe_id(<<"spiffe://td/foo/T/agent/A">>)).

parse_not_spiffe_test() ->
    ?assertMatch({error, _},
                 fm_common_peer:parse_spiffe_id(<<"https://example.com">>)).

ctx_from_spiffe_id_happy_test() ->
    Ctx = fm_common_peer:ctx_from_spiffe_id(
            <<"spiffe://faultmesh.local/ns/team-x/agent/h1">>),
    ?assertEqual(<<"team-x">>, fm_common_peer:tenant(Ctx)),
    ?assertEqual(<<"h1">>, fm_common_peer:agent_id(Ctx)),
    ?assertEqual(<<"spiffe://faultmesh.local/ns/team-x/agent/h1">>,
                 maps:get(spiffe, Ctx)).

ctx_from_spiffe_id_malformed_falls_back_test() ->
    Ctx = fm_common_peer:ctx_from_spiffe_id(<<"spiffe://bad">>),
    ?assertEqual(<<"default">>, fm_common_peer:tenant(Ctx)),
    ?assertEqual(<<"unknown">>, fm_common_peer:agent_id(Ctx)).
