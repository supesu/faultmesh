-module(fm_ingest_service).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([stream/2]).

stream(Ref, Stream) ->
    PeerCtx = fm_common_peer:ctx_from_grpc(Stream),
    Tenant = fm_common_peer:tenant(PeerCtx),
    AgentId = fm_common_peer:agent_id(PeerCtx),
    PartitionId = fm_ingest_router:partition_for(Tenant, AgentId),
    {ok, PartitionPid} = fm_ingest_partition_sup:start_partition(self(), PartitionId),
    logger:info(#{event => grpc_stream_accepted, tenant => Tenant, agent => AgentId,
                  partition_id => PartitionId, partition_pid => PartitionPid}),
    _ = telemetry:execute([fm_ingest, stream, accepted], #{count => 1},
                          #{tenant => Tenant, partition_id => PartitionId}),
    try
        loop(Ref, Stream, PeerCtx, PartitionPid)
    after
        catch fm_ingest_partition:stop(PartitionPid),
        _ = telemetry:execute([fm_ingest, stream, closed], #{count => 1}, #{})
    end.

loop(Ref, Stream, PeerCtx, Pid) ->
    receive
        {Ref, eos} ->
            ok;
        {Ref, #{offset := Off, payload := {event, EvMap}}} ->
            case fm_ingest_normalize:event(PeerCtx, EvMap) of
                {ok, Ev} ->
                    fm_ingest_partition:handle_request(Pid, {Off, Ev});
                {error, Reason} ->
                    _ = telemetry:execute([fm_ingest, normalize, error],
                                          #{count => 1}, #{reason => Reason})
            end,
            loop(Ref, Stream, PeerCtx, Pid);
        {Ref, Other} ->
            _ = telemetry:execute([fm_ingest, stream, unknown_frame], #{count => 1}, #{}),
            logger:debug(#{event => grpc_unknown_frame, frame => Other}),
            loop(Ref, Stream, PeerCtx, Pid);
        {send_ack, Off} ->
            send_ingest_ack(Stream, Off),
            loop(Ref, Stream, PeerCtx, Pid);
        Unexpected ->
            logger:warning(#{event => grpc_stream_unexpected, msg => Unexpected}),
            loop(Ref, Stream, PeerCtx, Pid)
    end.

send_ingest_ack(Stream, Off) ->
    Resp = #{payload => {ingest_ack, #{last_acked_offset => Off}}},
    try
        grpcbox_stream:send(Resp, Stream),
        _ = telemetry:execute([fm_ingest, ack, sent], #{count => 1}, #{offset => Off})
    catch
        Class:Reason ->
            _ = telemetry:execute([fm_ingest, ack, send_error], #{count => 1},
                                  #{class => Class, reason => Reason}),
            logger:warning(#{event => grpc_ack_send_failed, class => Class, reason => Reason})
    end.
