-module(fm_ingest_normalize).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([event/2]).

-spec event(peer_ctx(), map()) -> {ok, event()} | {error, term()}.
event(PeerCtx, Proto) when is_map(Proto) ->
    Tenant = fm_common_peer:tenant(PeerCtx),
    Agent  = resolve_agent_id(PeerCtx, Proto),
    TsNs   = ts_unix_ns(maps:get(timestamp, Proto, undefined)),
    Source = maps:get(source, Proto, undefined),
    {Kind, Payload} = normalize_payload(maps:get(payload, Proto, undefined)),
    {ok, #event{
        tenant_id  = Tenant,
        agent_id   = Agent,
        ts_unix_ns = TsNs,
        source     = Source,
        kind       = Kind,
        payload    = Payload
    }};
event(_PeerCtx, _Other) ->
    {error, not_a_map}.

resolve_agent_id(PeerCtx, Proto) ->
    case maps:get(agent_id, Proto, <<>>) of
        <<>> -> fm_common_peer:agent_id(PeerCtx);
        A when is_binary(A) -> A
    end.

ts_unix_ns(undefined) -> 0;
ts_unix_ns(#{seconds := S, nanos := N}) when is_integer(S), is_integer(N) ->
    (S * 1000000000) + N;
ts_unix_ns(_) -> 0.

normalize_payload({metric, #{} = M}) ->
    {metric, #{
        name   => maps:get(name, M, <<>>),
        value  => maps:get(value, M, 0.0),
        labels => maps:get(labels, M, #{})
    }};
normalize_payload(undefined) ->
    _ = telemetry:execute([fm_ingest, normalize, no_payload], #{count => 1}, #{}),
    {unknown, #{}};
normalize_payload({Tag, _}) ->
    _ = telemetry:execute([fm_ingest, normalize, unknown_oneof], #{count => 1}, #{tag => Tag}),
    {unknown, #{}};
normalize_payload(_) ->
    _ = telemetry:execute([fm_ingest, normalize, malformed_payload], #{count => 1}, #{}),
    {unknown, #{}}.
