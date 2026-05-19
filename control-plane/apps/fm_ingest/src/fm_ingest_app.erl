-module(fm_ingest_app).
-behaviour(application).

-export([start/2, stop/1]).

-dialyzer({nowarn_function, [start/2]}).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_ingest),
    _ = telemetry:execute([fm_ingest, app, start], #{count => 1}, #{}),
    ok = autodetect_spire(),
    {ok, Pid} = fm_ingest_sup:start_link(),
    ok = fm_ingest_listener:start(),
    {ok, Pid}.

autodetect_spire() ->
    CertPath = application:get_env(fm_ingest, svid_cert_path, "/run/spire/svid.pem"),
    case filelib:is_regular(CertPath) of
        true ->
            ok = application:set_env(fm_ingest, spire_enabled, true),
            logger:info(#{event => mtls_autodetected, svid_cert_path => CertPath});
        false ->
            ok
    end.

stop(_State) ->
    _ = fm_ingest_listener:stop(),
    ok.
