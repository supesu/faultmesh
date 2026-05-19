-module(fm_notify_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_notify),
    _ = telemetry:execute([fm_notify, app, start], #{count => 1}, #{}),
    fm_notify_sup:start_link().

stop(_State) ->
    ok.
