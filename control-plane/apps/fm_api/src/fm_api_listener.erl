-module(fm_api_listener).

-export([start/0]).

-dialyzer({nowarn_function, [start/0]}).

start() ->
    {ok, Port} = application:get_env(fm_api, grpc_port),
    {ok, IpStr} = application:get_env(fm_api, grpc_ip),
    {ok, Ip} = inet:parse_address(IpStr),
    TransportOpts = transport_opts(),
    ServerOpts = #{
        grpc_opts => grpc_opts(),
        listen_opts => #{port => Port, ip => Ip},
        pool_opts      => #{size => 4},
        transport_opts => TransportOpts
    },
    case grpcbox:start_server(ServerOpts) of
        {ok, _Pid} ->
            logger:info(#{event => fm_api_listener_started, port => Port,
                          tls => maps:get(ssl, TransportOpts, false)}),
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        Err ->
            logger:error(#{event => fm_api_listener_failed, error => Err}),
            Err
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
             services       => #{'faultmesh.v1.DebugService' => fm_api_debug_service}},
    case application:get_env(fm_ingest, spire_enabled, false) of
        true  -> Base#{auth_fun => fun spiffe_auth/1};
        false -> Base
    end.

spiffe_auth(Cert) ->
    case fm_common_peer:spiffe_id_from_cert(Cert) of
        {ok, SpiffeId} -> {true, {spiffe, SpiffeId}};
        {error, _} ->
            case fm_common_peer:cn_from_cert(Cert) of
                {ok, CN}   -> {true, {cn, CN}};
                {error, _} -> {true, undefined}
            end
    end.
