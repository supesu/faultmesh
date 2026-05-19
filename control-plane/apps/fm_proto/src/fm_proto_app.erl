-module(fm_proto_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_proto),
    _ = telemetry:execute([fm_proto, app, start], #{count => 1}, #{}),
    fm_proto_sup:start_link().

stop(_State) ->
    ok.
