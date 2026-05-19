-module(fm_storage_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_storage),
    _ = telemetry:execute([fm_storage, app, start], #{count => 1}, #{}),
    fm_storage_sup:start_link().

stop(_State) ->
    ok.
