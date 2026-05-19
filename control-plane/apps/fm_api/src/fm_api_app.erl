-module(fm_api_app).
-behaviour(application).

-export([start/2, stop/1]).

-dialyzer({nowarn_function, [start/2]}).

start(_StartType, _StartArgs) ->
    ok = fm_common_metrics:register_up_gauge(fm_api),
    _ = telemetry:execute([fm_api, app, start], #{count => 1}, #{}),
    ok = fm_api_tail_fanout:start(),
    ok = attach_sink_bridge(),
    {ok, Pid} = fm_api_sup:start_link(),
    ok = fm_api_listener:start(),
    {ok, Pid}.

stop(_State) ->
    ok.

attach_sink_bridge() ->
    telemetry:attach(
      <<"fm_api_tail_bridge">>,
      [fm_ingest, sink, accepted],
      fun(_Event, _Measurements, Meta, _Config) ->
              case Meta of
                  #{offset := Off, event := Ev} ->
                      fm_api_tail_fanout:publish(Off, Ev);
                  _ -> ok
              end
      end,
      undefined),
    ok.
