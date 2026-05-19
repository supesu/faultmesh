-module(fm_common_metrics_handler).

-export([init/2]).

init(Req0, State) ->
    Body = prometheus_text_format:format(),
    Req = cowboy_req:reply(200,
        #{<<"content-type">> => <<"text/plain; version=0.0.4">>},
        Body, Req0),
    {ok, Req, State}.
