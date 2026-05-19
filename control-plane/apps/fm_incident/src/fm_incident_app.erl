-module(fm_incident_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_incident),
    _ = telemetry:execute([fm_incident, app, start], #{count => 1}, #{}),
    fm_incident_sup:start_link().

stop(_State) ->
    ok.
