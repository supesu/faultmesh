
ARG OTP_VERSION=27

FROM erlang:${OTP_VERSION} AS build

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends git build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY control-plane/rebar.config control-plane/rebar.lock* ./
COPY control-plane/apps ./apps
COPY control-plane/config ./config
COPY control-plane/_checkouts ./_checkouts
COPY proto /proto

RUN rebar3 as prod tar \
    && mkdir -p /opt/faultmesh \
    && tar -xzf _build/prod/rel/faultmesh/faultmesh-*.tar.gz -C /opt/faultmesh

FROM debian:12-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 libncurses6 openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --no-create-home faultmesh

COPY --from=build /opt/faultmesh /opt/faultmesh

USER faultmesh
EXPOSE 9100
ENTRYPOINT ["/opt/faultmesh/bin/faultmesh"]
CMD ["foreground"]
