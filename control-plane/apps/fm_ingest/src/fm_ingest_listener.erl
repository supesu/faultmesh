-module(fm_ingest_listener).

-export([start/0, stop/0, reload_tls/0]).

-dialyzer({nowarn_function, [start/0, reload_tls/0, start_server/0]}).

start() ->
    start_server().

stop() ->
    ok.

reload_tls() ->
    case application:get_env(fm_ingest, spire_enabled, false) of
        true ->
            stop_server(),
            _ = start_server(),
            ok;
        false ->
            ok
    end.

start_server() ->
    {ok, Port} = application:get_env(fm_ingest, grpc_port),
    {ok, IpStr} = application:get_env(fm_ingest, grpc_ip),
    {ok, Ip} = inet:parse_address(IpStr),
    TransportOpts = transport_opts(),
    GrpcOpts = grpc_opts(),
    ServerOpts = #{
        grpc_opts      => GrpcOpts,
        listen_opts    => #{port => Port, ip => Ip},
        pool_opts      => #{size => 8},
        transport_opts => TransportOpts
    },
    case grpcbox:start_server(ServerOpts) of
        {ok, _Pid} ->
            logger:info(#{event => grpc_listener_started, port => Port,
                          tls => maps:get(ssl, TransportOpts, false)}),
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        Err ->
            logger:error(#{event => grpc_listener_failed, error => Err}),
            Err
    end.

stop_server() ->
    case whereis(grpcbox_services_simple_sup) of
        undefined -> ok;
        _ ->
            [supervisor:terminate_child(grpcbox_services_simple_sup, Id)
             || {Id, _, _, _} <- supervisor:which_children(grpcbox_services_simple_sup)],
            ok
    end.

transport_opts() ->
    case application:get_env(fm_ingest, spire_enabled, false) of
        true ->
            TLS = fm_ingest_spiffe:get_tls_opts(),
            maps:from_list([{ssl, true} | TLS]);
        false ->
            #{}
    end.

grpc_opts() ->
    Base = #{service_protos => [control_pb],
             services       => #{'faultmesh.v1.AgentControlService' => fm_ingest_service}},
    case application:get_env(fm_ingest, spire_enabled, false) of
        true  -> Base#{auth_fun => fun spiffe_auth/1};
        false -> Base
    end.

spiffe_auth(Cert) ->
    case fm_common_peer:spiffe_id_from_cert(Cert) of
        {ok, SpiffeId} ->
            {true, {spiffe, SpiffeId}};
        {error, _} ->
            case fm_common_peer:cn_from_cert(Cert) of
                {ok, CN}   -> {true, {cn, CN}};
                {error, _} -> {true, undefined}
            end
    end.
