-module(fm_common_logger).

-export([install/0, format/2]).

install() ->
    ok = logger:set_handler_config(default, formatter, {?MODULE, #{}}),
    ok.

format(#{level := Level, msg := Msg, meta := Meta}, _Config) ->
    Base = #{
        <<"ts">>    => iso8601_now(),
        <<"level">> => atom_to_binary(Level, utf8),
        <<"msg">>   => render_msg(Msg)
    },
    Extra = maps:without([gl, time, pid, mfa, file, line, domain, report_cb], Meta),
    [jsx:encode(maps:merge(Base, ensure_jsonable(Extra))), $\n].

render_msg({string, S}) ->
    unicode:characters_to_binary(S);
render_msg({report, R}) when is_map(R) ->
    ensure_jsonable(R);
render_msg({report, R}) when is_list(R) ->
    ensure_jsonable(maps:from_list(R));
render_msg({Fmt, Args}) ->
    unicode:characters_to_binary(io_lib:format(Fmt, Args)).

ensure_jsonable(M) when is_map(M) ->
    maps:map(fun(_K, V) -> stringify(V) end, M).

stringify(V) when is_atom(V) -> atom_to_binary(V, utf8);
stringify(V) when is_pid(V)  -> list_to_binary(pid_to_list(V));
stringify(V) when is_reference(V) -> list_to_binary(ref_to_list(V));
stringify(V) when is_binary(V); is_number(V); is_boolean(V) -> V;
stringify(V) when is_list(V) ->
    try unicode:characters_to_binary(V) of
        B when is_binary(B) -> B;
        _ -> list_to_binary(io_lib:format("~p", [V]))
    catch _:_ -> list_to_binary(io_lib:format("~p", [V])) end;
stringify(V) when is_map(V) -> ensure_jsonable(V);
stringify(V) -> list_to_binary(io_lib:format("~p", [V])).

iso8601_now() ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(erlang:system_time(second), second),
    iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                                   [Y, Mo, D, H, Mi, S])).
