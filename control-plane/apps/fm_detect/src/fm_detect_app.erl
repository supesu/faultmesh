-module(fm_detect_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_detect),
    _ = telemetry:execute([fm_detect, app, start], #{count => 1}, #{}),
    fm_detect_sup:start_link().

stop(_State) ->
    ok.
