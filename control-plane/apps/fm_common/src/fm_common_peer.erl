-module(fm_common_peer).

-include("fm_common_types.hrl").

-export([dev_ctx/0, ctx_from_grpc/1,
         ctx_from_spiffe_id/1, parse_spiffe_id/1,
         spiffe_id_from_cert/1, cn_from_cert/1,
         tenant/1, agent_id/1]).

-spec dev_ctx() -> peer_ctx().
dev_ctx() ->
    #{tenant => <<"default">>, agent_id => <<"unknown">>,
      spiffe => undefined, peer => undefined}.

-spec ctx_from_grpc(term()) -> peer_ctx().
ctx_from_grpc(Stream) ->
    case safe_peer_identity(Stream) of
        {spiffe, SpiffeId} ->
            ctx_from_spiffe_id(SpiffeId);
        {cn, CN} ->
            (dev_ctx())#{agent_id => CN};
        _ ->
            dev_ctx()
    end.

safe_peer_identity(Stream) ->
    try grpcbox_stream:peer_identity(Stream)
    catch _:_ -> undefined end.

-spec ctx_from_spiffe_id(binary()) -> peer_ctx().
ctx_from_spiffe_id(SpiffeId) when is_binary(SpiffeId) ->
    case parse_spiffe_id(SpiffeId) of
        {ok, _TD, Tenant, Agent} ->
            #{tenant => Tenant, agent_id => Agent,
              spiffe => SpiffeId, peer => undefined};
        {error, _} ->
            (dev_ctx())#{spiffe => SpiffeId}
    end.

-spec parse_spiffe_id(binary()) ->
    {ok, binary(), binary(), binary()} | {error, term()}.
parse_spiffe_id(<<"spiffe://", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, [global]) of
        [TD, <<"ns">>, Tenant, <<"agent">>, Agent | _]
          when byte_size(TD) > 0, byte_size(Tenant) > 0, byte_size(Agent) > 0 ->
            {ok, TD, Tenant, Agent};
        _ ->
            {error, malformed}
    end;
parse_spiffe_id(_) ->
    {error, not_a_spiffe_uri}.

-spec spiffe_id_from_cert(binary()) -> {ok, binary()} | {error, term()}.
spiffe_id_from_cert(DerBin) when is_binary(DerBin) ->
    try
        OTPCert = public_key:pkix_decode_cert(DerBin, otp),
        TBS = element(2, OTPCert),
        Exts = element(11, TBS),
        case find_san_uri(Exts) of
            {ok, Uri} -> {ok, Uri};
            error     -> {error, no_spiffe_uri}
        end
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

find_san_uri(asn1_NOVALUE) -> error;
find_san_uri(undefined)    -> error;
find_san_uri(Exts) when is_list(Exts) ->
    SanOid = {2,5,29,17},
    case lists:keyfind(SanOid, 2, Exts) of
        false -> error;
        Ext   ->
            Vals = element(4, Ext),
            scan_uris(Vals)
    end.

scan_uris([]) -> error;
scan_uris([{uniformResourceIdentifier, Uri} | Rest]) ->
    UriBin = unicode:characters_to_binary(Uri),
    case UriBin of
        <<"spiffe://", _/binary>> -> {ok, UriBin};
        _ -> scan_uris(Rest)
    end;
scan_uris([_ | Rest]) -> scan_uris(Rest).

-spec cn_from_cert(binary()) -> {ok, binary()} | {error, term()}.
cn_from_cert(DerBin) when is_binary(DerBin) ->
    try
        OTPCert = public_key:pkix_decode_cert(DerBin, otp),
        TBS = element(2, OTPCert),
        SubjectRdn = element(7, TBS),
        {rdnSequence, Rdns} = SubjectRdn,
        case find_cn(Rdns) of
            {ok, CN} -> {ok, CN};
            error    -> {error, no_cn}
        end
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

find_cn([]) -> error;
find_cn([Rdn | Rest]) ->
    CNOid = {2,5,4,3},
    case lists:keyfind(CNOid, 2, Rdn) of
        false -> find_cn(Rest);
        AttrTypeAndValue ->
            Val = element(3, AttrTypeAndValue),
            {ok, attr_value_to_binary(Val)}
    end.

attr_value_to_binary({utf8String, B}) when is_binary(B) -> B;
attr_value_to_binary({printableString, S})              -> unicode:characters_to_binary(S);
attr_value_to_binary({teletexString, S})                -> unicode:characters_to_binary(S);
attr_value_to_binary({ia5String, S})                    -> unicode:characters_to_binary(S);
attr_value_to_binary(B) when is_binary(B)               -> B;
attr_value_to_binary(S) when is_list(S)                 -> unicode:characters_to_binary(S).

-spec tenant(peer_ctx()) -> binary().
tenant(#{tenant := T}) when is_binary(T), byte_size(T) > 0 -> T;
tenant(_) -> <<"default">>.

-spec agent_id(peer_ctx()) -> binary().
agent_id(#{agent_id := A}) when is_binary(A), byte_size(A) > 0 -> A;
agent_id(_) -> <<"unknown">>.
