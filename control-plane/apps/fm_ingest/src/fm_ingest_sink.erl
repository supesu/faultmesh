-module(fm_ingest_sink).
-behaviour(gen_server).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([start_link/0, deliver/3, pause/0, resume/0, paused/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(s, {
    paused = false :: boolean(),
    pending = queue:new() :: queue:queue()
}).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec deliver(pid(), non_neg_integer(), event()) -> ok.
deliver(From, Offset, Event) ->
    gen_server:cast(?MODULE, {deliver, From, Offset, Event}).

-spec pause() -> ok.
pause() -> gen_server:call(?MODULE, pause).

-spec resume() -> ok.
resume() -> gen_server:call(?MODULE, resume).

-spec paused() -> boolean().
paused() -> gen_server:call(?MODULE, paused).

init([]) ->
    {ok, #s{}}.

handle_call(pause, _From, S) ->
    {reply, ok, S#s{paused = true}};
handle_call(resume, _From, S) ->
    S1 = drain(S#s{paused = false}),
    {reply, ok, S1};
handle_call(paused, _From, S) ->
    {reply, S#s.paused, S}.

handle_cast({deliver, From, Off, Ev}, S = #s{paused = true, pending = Q}) ->
    _ = telemetry:execute([fm_ingest, sink, buffered], #{count => 1}, #{}),
    {noreply, S#s{pending = queue:in({From, Off, Ev}, Q)}};
handle_cast({deliver, From, Off, Ev}, S = #s{paused = false}) ->
    ack(From, Off, Ev),
    {noreply, S}.

handle_info(_Msg, S) -> {noreply, S}.
terminate(_R, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

drain(S = #s{pending = Q}) ->
    case queue:out(Q) of
        {empty, _} ->
            S;
        {{value, {From, Off, Ev}}, Q1} ->
            ack(From, Off, Ev),
            drain(S#s{pending = Q1})
    end.

ack(From, Off, #event{kind = Kind} = Ev) ->
    _ = telemetry:execute([fm_ingest, sink, accepted],
                          #{count => 1},
                          #{kind => Kind, offset => Off, event => Ev}),
    From ! {sink_acked, Off}.
