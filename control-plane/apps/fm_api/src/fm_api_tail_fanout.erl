-module(fm_api_tail_fanout).

-include_lib("fm_common/src/fm_common_types.hrl").

-export([start/0, subscribe/2, unsubscribe/2, publish/2]).

-define(SCOPE, fm_api_tail).

start() ->
    case pg:start_link(?SCOPE) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

-spec subscribe(binary(), binary()) -> ok.
subscribe(Tenant, AgentId) ->
    pg:join(?SCOPE, group_key(Tenant, AgentId), self()).

-spec unsubscribe(binary(), binary()) -> ok.
unsubscribe(Tenant, AgentId) ->
    pg:leave(?SCOPE, group_key(Tenant, AgentId), self()).

-spec publish(non_neg_integer(), event()) -> ok.
publish(Offset, #event{tenant_id = T, agent_id = A} = Ev) ->
    Msg = {fm_api_tail, Offset, Ev},
    Keys = [group_key(T, A), group_key(T, <<>>), group_key(<<>>, <<>>)],
    lists:foreach(
      fun(K) ->
          [Pid ! Msg || Pid <- pg:get_members(?SCOPE, K)]
      end, Keys),
    ok.

group_key(Tenant, AgentId) ->
    {tail, Tenant, AgentId}.
