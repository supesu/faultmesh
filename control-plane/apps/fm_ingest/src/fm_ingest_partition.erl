-module(fm_ingest_partition).
-behaviour(gen_statem).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([start_link/2, handle_request/2, stop/1]).
-export([init/1, callback_mode/0, terminate/3]).
-export([open/3, throttled/3]).

-define(DEFAULT_WINDOW, 1024).

-record(d, {
    handler_pid  :: pid(),
    partition_id :: non_neg_integer(),
    window       :: pos_integer(),
    in_flight = 0 :: non_neg_integer(),
    pending = queue:new() :: queue:queue(),
    last_acked = 0 :: non_neg_integer()
}).

start_link(HandlerPid, PartitionId) ->
    gen_statem:start_link(?MODULE, [HandlerPid, PartitionId], []).

handle_request(Pid, Req) ->
    gen_statem:cast(Pid, {request, Req}).

stop(Pid) ->
    gen_statem:stop(Pid, normal, 5000).

callback_mode() -> [state_functions, state_enter].

init([HandlerPid, PartitionId]) ->
    Window = application:get_env(fm_ingest, credit_window, ?DEFAULT_WINDOW),
    _ = fm_ingest_wal:ensure(PartitionId),
    {ok, open, #d{handler_pid = HandlerPid,
                  partition_id = PartitionId,
                  window = Window}}.

open(enter, _Old, _D) ->
    keep_state_and_data;

open(cast, {request, _}, D = #d{in_flight = N, window = W}) when N >= W ->
    _ = telemetry:execute([fm_ingest, partition, throttled], #{count => 1}, #{}),
    {next_state, throttled, D, [postpone]};

open(cast, {request, {Off, _Ev}}, D = #d{last_acked = LA}) when Off =< LA ->
    notify_handler_ack(D, LA),
    _ = telemetry:execute([fm_ingest, partition, replay_drop], #{count => 1}, #{}),
    keep_state_and_data;

open(cast, {request, {Off, Ev}}, D) ->
    ok = fm_ingest_wal:append(D#d.partition_id, Off, Ev),
    ok = fm_ingest_sink:deliver(self(), Off, Ev),
    Q1 = queue:in(Off, D#d.pending),
    {keep_state, D#d{in_flight = D#d.in_flight + 1, pending = Q1}};

open(info, {sink_acked, Off}, D) ->
    {keep_state, on_sink_ack(Off, D)};

open({call, From}, _Msg, _D) ->
    {keep_state_and_data, [{reply, From, ok}]}.

throttled(enter, _Old, _D) ->
    keep_state_and_data;

throttled(cast, {request, _}, _D) ->
    {keep_state_and_data, [postpone]};

throttled(info, {sink_acked, Off}, D) ->
    D1 = on_sink_ack(Off, D),
    case D1#d.in_flight < D1#d.window of
        true  -> {next_state, open, D1};
        false -> {keep_state, D1}
    end;

throttled({call, From}, _Msg, _D) ->
    {keep_state_and_data, [{reply, From, ok}]}.

terminate(_Reason, _State, _D) ->
    ok.

on_sink_ack(Off, D = #d{pending = Q, in_flight = N}) ->
    Q1 = case queue:out(Q) of
             {{value, Off}, Rest} -> Rest;
             {{value, Other}, Rest} ->
                 _ = telemetry:execute([fm_ingest, partition, ack_out_of_order],
                                       #{count => 1}, #{got => Off, expected => Other}),
                 Rest;
             {empty, Empty} -> Empty
         end,
    D1 = D#d{pending = Q1, in_flight = max(0, N - 1), last_acked = Off},
    notify_handler_ack(D1, Off),
    _ = telemetry:execute([fm_ingest, partition, acked], #{count => 1}, #{offset => Off}),
    D1.

notify_handler_ack(#d{handler_pid = Pid}, Off) ->
    Pid ! {send_ack, Off},
    ok.
