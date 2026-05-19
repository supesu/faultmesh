-module(fm_ingest_wal).
-behaviour(gen_server).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([start_link/1, ensure/1, append/3, tip/1, sync/1, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(WRAP_FILE_BYTES, 268435456).
-define(WRAP_FILE_COUNT, 16).

-record(s, {
    partition_id :: non_neg_integer(),
    log_name     :: atom(),
    tip = 0      :: non_neg_integer()
}).

-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(PartId) ->
    gen_server:start_link(?MODULE, [PartId], []).

-spec ensure(non_neg_integer()) -> pid().
ensure(PartId) ->
    Key = {n, l, {?MODULE, PartId}},
    case gproc:where(Key) of
        undefined ->
            {ok, Pid} = fm_ingest_wal_sup:start_wal(PartId),
            gproc:reg_other(Key, Pid),
            Pid;
        Pid when is_pid(Pid) ->
            Pid
    end.

-spec append(non_neg_integer(), non_neg_integer(), event()) -> ok.
append(PartId, Offset, Event) ->
    gen_server:cast(ensure(PartId), {append, Offset, Event}).

-spec tip(non_neg_integer()) -> non_neg_integer().
tip(PartId) ->
    gen_server:call(ensure(PartId), tip).

-spec sync(non_neg_integer()) -> ok.
sync(PartId) ->
    gen_server:call(ensure(PartId), sync).

-spec close(non_neg_integer()) -> ok.
close(PartId) ->
    case gproc:where({n, l, {?MODULE, PartId}}) of
        undefined -> ok;
        Pid -> gen_server:call(Pid, close)
    end.

init([PartId]) ->
    process_flag(trap_exit, true),
    Dir = wal_dir_for(PartId),
    ok = filelib:ensure_dir(filename:join(Dir, "marker")),
    LogName = list_to_atom("fm_ingest_wal_" ++ integer_to_list(PartId)),
    File = filename:join(Dir, "wal"),
    Args = [{name, LogName}, {file, File}, {type, wrap}, {format, internal},
            {size, {?WRAP_FILE_BYTES, ?WRAP_FILE_COUNT}}, {mode, read_write}],
    case disk_log:open(Args) of
        {ok, _} ->
            Tip = scan_tip(LogName),
            _ = telemetry:execute([fm_ingest, wal, opened], #{count => 1},
                                  #{partition_id => PartId, tip => Tip}),
            {ok, #s{partition_id = PartId, log_name = LogName, tip = Tip}};
        {repaired, _, _, _} ->
            Tip = scan_tip(LogName),
            _ = telemetry:execute([fm_ingest, wal, repaired], #{count => 1},
                                  #{partition_id => PartId, tip => Tip}),
            {ok, #s{partition_id = PartId, log_name = LogName, tip = Tip}};
        Err ->
            {stop, {wal_open_failed, Err}}
    end.

handle_call(tip, _From, S) ->
    {reply, S#s.tip, S};
handle_call(sync, _From, S = #s{log_name = LN}) ->
    {reply, disk_log:sync(LN), S};
handle_call(close, _From, S = #s{log_name = LN}) ->
    _ = disk_log:close(LN),
    {stop, normal, ok, S}.

handle_cast({append, Off, Event}, S = #s{log_name = LN, tip = Tip}) ->
    case disk_log:log(LN, {Off, Event}) of
        ok ->
            NewTip = max(Tip, Off),
            _ = telemetry:execute([fm_ingest, wal, appended], #{count => 1},
                                  #{partition_id => S#s.partition_id, offset => Off}),
            {noreply, S#s{tip = NewTip}};
        Err ->
            _ = telemetry:execute([fm_ingest, wal, append_error], #{count => 1},
                                  #{partition_id => S#s.partition_id, error => Err}),
            logger:warning(#{event => wal_append_failed,
                             partition_id => S#s.partition_id, error => Err}),
            {noreply, S}
    end.

handle_info(_Msg, S) -> {noreply, S}.

terminate(_Reason, #s{log_name = LN}) ->
    _ = disk_log:close(LN),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

wal_dir_for(PartId) ->
    Root = application:get_env(fm_ingest, wal_dir, "/var/lib/faultmesh/wal"),
    filename:join(Root, integer_to_list(PartId)).

scan_tip(LogName) ->
    scan_tip(LogName, start, 0).

scan_tip(LogName, Cont, Acc) ->
    case disk_log:chunk(LogName, Cont) of
        eof ->
            Acc;
        {error, _} = E ->
            logger:warning(#{event => wal_scan_error, error => E}),
            Acc;
        {NextCont, Terms} ->
            Max = lists:foldl(fun({Off, _}, A) -> max(A, Off);
                                 (_, A) -> A
                              end, Acc, Terms),
            scan_tip(LogName, NextCont, Max);
        {NextCont, Terms, _Bad} ->
            Max = lists:foldl(fun({Off, _}, A) -> max(A, Off);
                                 (_, A) -> A
                              end, Acc, Terms),
            scan_tip(LogName, NextCont, Max)
    end.
