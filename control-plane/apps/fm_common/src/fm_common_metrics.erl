-module(fm_common_metrics).

-export([init/0, start_http/1, register_up_gauge/1]).

-define(METRICS_DISPATCH, fm_common_metrics_dispatch).

init() ->
    ok.

start_http(Port) ->
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/metrics", fm_common_metrics_handler, []}
        ]}
    ]),
    cowboy:start_clear(
        fm_common_metrics_listener,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ).

register_up_gauge(AppName) when is_atom(AppName) ->
    Name = list_to_atom(atom_to_list(AppName) ++ "_up"),
    Help = "1 if " ++ atom_to_list(AppName) ++ " is running",
    _ = prometheus_gauge:declare([{name, Name}, {help, Help}]),
    prometheus_gauge:set(Name, 1),
    ok.
