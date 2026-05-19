-module(fm_ingest_spiffe).
-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").

-export([start_link/0, get_tls_opts/0, trust_domain/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(POLL_INTERVAL_MS, 5000).

-record(s, {
    cert_path :: string(),
    key_path  :: string(),
    ca_path   :: string(),
    trust_domain :: binary(),
    mtimes    :: #{atom() => integer()},
    tls_opts = [] :: list()
}).

-spec start_link() -> {ok, pid()} | ignore.
start_link() ->
    case application:get_env(fm_ingest, spire_enabled, false) of
        true  -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []);
        false -> ignore
    end.

-spec get_tls_opts() -> list().
get_tls_opts() ->
    persistent_term:get({?MODULE, tls_opts}, []).

-spec trust_domain() -> binary().
trust_domain() ->
    application:get_env(fm_ingest, trust_domain, <<"faultmesh.local">>).

init([]) ->
    CertPath = application:get_env(fm_ingest, svid_cert_path, "/run/spire/svid.pem"),
    KeyPath  = application:get_env(fm_ingest, svid_key_path,  "/run/spire/svid.key"),
    CaPath   = application:get_env(fm_ingest, svid_bundle_path,"/run/spire/bundle.pem"),
    TD       = trust_domain(),
    S0 = #s{cert_path = CertPath, key_path = KeyPath, ca_path = CaPath,
            trust_domain = TD, mtimes = #{}},
    case load(S0) of
        {ok, S1} ->
            erlang:send_after(?POLL_INTERVAL_MS, self(), poll),
            _ = telemetry:execute([fm_ingest, spiffe, loaded], #{count => 1},
                                  #{trust_domain => TD}),
            {ok, S1};
        {error, Reason} ->
            logger:error(#{event => spiffe_load_failed, reason => Reason,
                           cert_path => CertPath}),
            {stop, {spiffe_load_failed, Reason}}
    end.

handle_call(_, _From, S) ->
    {reply, ok, S}.

handle_cast(_, S) -> {noreply, S}.

handle_info(poll, S) ->
    S1 = case mtimes_changed(S) of
             true ->
                 case load(S) of
                     {ok, S2} ->
                         _ = telemetry:execute([fm_ingest, spiffe, reloaded],
                                               #{count => 1}, #{}),
                         logger:info(#{event => spiffe_reloaded}),
                         fm_ingest_listener:reload_tls(),
                         S2;
                     {error, R} ->
                         logger:warning(#{event => spiffe_reload_failed, reason => R}),
                         S
                 end;
             false ->
                 S
         end,
    erlang:send_after(?POLL_INTERVAL_MS, self(), poll),
    {noreply, S1};
handle_info(_, S) -> {noreply, S}.

terminate(_R, _S) -> ok.
code_change(_O, S, _E) -> {ok, S}.

load(S) ->
    try
        Mtimes = #{cert => mtime(S#s.cert_path),
                   key  => mtime(S#s.key_path),
                   ca   => mtime(S#s.ca_path)},
        TLSOpts = [
            {certfile, S#s.cert_path},
            {keyfile,  S#s.key_path},
            {cacertfile, S#s.ca_path},
            {verify, verify_peer},
            {fail_if_no_peer_cert, true},
            {versions, ['tlsv1.3', 'tlsv1.2']},
            {alpn_preferred_protocols, [<<"h2">>]}
        ],
        persistent_term:put({?MODULE, tls_opts}, TLSOpts),
        {ok, S#s{mtimes = Mtimes, tls_opts = TLSOpts}}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

mtime(Path) ->
    case file:read_file_info(Path) of
        {ok, FI}   -> FI#file_info.mtime;
        {error, R} -> erlang:error({read_file_info, Path, R})
    end.

mtimes_changed(S) ->
    try
        Cur = #{cert => mtime(S#s.cert_path),
                key  => mtime(S#s.key_path),
                ca   => mtime(S#s.ca_path)},
        Cur =/= S#s.mtimes
    catch _:_ -> false end.
