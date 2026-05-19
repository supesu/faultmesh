-module(fm_ingest_normalize_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("fm_common/src/fm_common_types.hrl").

metric_event_test() ->
    Ctx = fm_common_peer:dev_ctx(),
    Proto = #{
        timestamp => #{seconds => 1700000000, nanos => 123},
        source    => <<"node-exporter">>,
        agent_id  => <<"agent-1">>,
        payload   => {metric, #{name => <<"cpu.util">>, value => 0.42, labels => #{<<"host">> => <<"h1">>}}}
    },
    {ok, Ev} = fm_ingest_normalize:event(Ctx, Proto),
    ?assertEqual(<<"default">>, Ev#event.tenant_id),
    ?assertEqual(<<"agent-1">>, Ev#event.agent_id),
    ?assertEqual(1700000000 * 1000000000 + 123, Ev#event.ts_unix_ns),
    ?assertEqual(<<"node-exporter">>, Ev#event.source),
    ?assertEqual(metric, Ev#event.kind),
    ?assertMatch(#{name := <<"cpu.util">>, value := 0.42}, Ev#event.payload).

agent_id_falls_back_to_peer_ctx_test() ->
    Ctx = (fm_common_peer:dev_ctx())#{agent_id => <<"peer-derived">>},
    Proto = #{payload => undefined, agent_id => <<>>},
    {ok, Ev} = fm_ingest_normalize:event(Ctx, Proto),
    ?assertEqual(<<"peer-derived">>, Ev#event.agent_id).

unknown_oneof_does_not_crash_test() ->
    Ctx = fm_common_peer:dev_ctx(),
    Proto = #{payload => {future_payload, #{anything => 1}}},
    {ok, Ev} = fm_ingest_normalize:event(Ctx, Proto),
    ?assertEqual(unknown, Ev#event.kind),
    ?assertEqual(#{}, Ev#event.payload).

missing_timestamp_defaults_to_zero_test() ->
    Ctx = fm_common_peer:dev_ctx(),
    {ok, Ev} = fm_ingest_normalize:event(Ctx, #{payload => undefined}),
    ?assertEqual(0, Ev#event.ts_unix_ns).

non_map_input_test() ->
    Ctx = fm_common_peer:dev_ctx(),
    ?assertEqual({error, not_a_map}, fm_ingest_normalize:event(Ctx, not_a_map)).
