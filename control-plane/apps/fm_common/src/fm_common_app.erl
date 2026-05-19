-module(fm_common_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = fm_common_logger:install(),
    ok = fm_common_metrics:init(),
    {ok, Port} = application:get_env(fm_common, metrics_port),
    {ok, _} = fm_common_metrics:start_http(Port),
    ok = fm_common_metrics:register_up_gauge(fm_common),
    fm_common_sup:start_link().

stop(_State) ->
    ok.
