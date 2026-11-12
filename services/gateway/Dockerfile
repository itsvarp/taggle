FROM elixir:1.20.5-otp-29-slim AS builder
WORKDIR /app
ENV MIX_ENV=prod

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
RUN mix deps.get --only prod && mix deps.compile

COPY lib lib
RUN mix compile && mix release

FROM debian:trixie-slim
ENV LANG=C.UTF-8
RUN apt-get update && apt-get install -y --no-install-recommends \
      libstdc++6 openssl libncurses6 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin taggle

WORKDIR /app
COPY --from=builder --chown=taggle:taggle /app/_build/prod/rel/taggle_gateway ./
USER taggle
EXPOSE 4000
CMD ["/app/bin/taggle_gateway", "start"]
