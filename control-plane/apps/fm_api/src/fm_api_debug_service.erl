-module(fm_api_debug_service).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([tail_events/2]).

tail_events(#{tenant := T, agent_id := A}, Stream) ->
    Tenant  = ensure_bin(T),
    AgentId = ensure_bin(A),
    ok = fm_api_tail_fanout:subscribe(Tenant, AgentId),
    logger:info(#{event => debug_tail_started, tenant => Tenant, agent_id => AgentId}),
    _ = telemetry:execute([fm_api, debug_tail, started], #{count => 1},
                          #{tenant => Tenant, agent_id => AgentId}),
    try
        loop(Stream)
    after
        fm_api_tail_fanout:unsubscribe(Tenant, AgentId),
        _ = telemetry:execute([fm_api, debug_tail, ended], #{count => 1}, #{})
    end.

loop(Stream) ->
    receive
        {fm_api_tail, Offset, Event} ->
            EventProto = event_to_proto(Event),
            grpcbox_stream:send(#{offset => Offset, event => EventProto}, Stream),
            loop(Stream);
        stop ->
            ok;
        Other ->
            logger:debug(#{event => debug_tail_unexpected, msg => Other}),
            loop(Stream)
    end.

ensure_bin(undefined) -> <<>>;
ensure_bin(B) when is_binary(B) -> B;
ensure_bin(L) when is_list(L) -> list_to_binary(L).

event_to_proto(#event{tenant_id = _T, agent_id = A, ts_unix_ns = TsNs,
                      source = Src, kind = metric, payload = P}) ->
    base_proto(A, TsNs, Src, {metric, metric_to_proto(P)});
event_to_proto(#event{tenant_id = _T, agent_id = A, ts_unix_ns = TsNs,
                      source = Src}) ->
    base_proto(A, TsNs, Src, undefined).

base_proto(A, TsNs, Src, PayloadOpt) ->
    Base = #{agent_id => A,
             source   => default_bin(Src),
             timestamp => ns_to_timestamp(TsNs)},
    case PayloadOpt of
        undefined -> Base;
        Payload   -> Base#{payload => Payload}
    end.

metric_to_proto(P) when is_map(P) ->
    #{name   => maps:get(name, P, <<>>),
      value  => maps:get(value, P, 0.0),
      labels => maps:get(labels, P, #{})}.

default_bin(undefined) -> <<>>;
default_bin(V) -> V.

ns_to_timestamp(0) -> #{seconds => 0, nanos => 0};
ns_to_timestamp(Ns) ->
    #{seconds => Ns div 1000000000, nanos => Ns rem 1000000000}.
