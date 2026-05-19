
-ifndef(FM_COMMON_TYPES_HRL).
-define(FM_COMMON_TYPES_HRL, true).

-type tenant_id()   :: binary().
-type agent_id()    :: binary().
-type incident_id() :: binary().
-type detector_id() :: binary().

-type event_kind() :: metric | unknown.

-record(event, {
    tenant_id  :: tenant_id(),
    agent_id   :: agent_id(),
    ts_unix_ns :: non_neg_integer(),
    source     :: binary() | undefined,
    kind       :: event_kind(),
    payload    :: map()
}).

-type peer_ctx() :: #{
    tenant   => binary(),
    agent_id => binary(),
    spiffe   => binary() | undefined,
    peer     => term()
}.

-record(signal, {
    tenant_id   :: tenant_id(),
    detector_id :: detector_id(),
    entity      :: {binary(), binary(), binary(), binary()},
    severity    :: info | warning | critical,
    ts_unix_ns  :: non_neg_integer(),
    metric      :: binary(),
    value       :: number(),
    metadata    :: map()
}).

-type event()  :: #event{}.
-type signal() :: #signal{}.

-endif.
