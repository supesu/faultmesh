-module(fm_workflow_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_workflow),
    _ = telemetry:execute([fm_workflow, app, start], #{count => 1}, #{}),
    fm_workflow_sup:start_link().

stop(_State) ->
    ok.
