ARG UV_VERSION=0.5.13
ARG DEBIAN_VERSION=bookworm

FROM ghcr.io/astral-sh/uv:$UV_VERSION AS uv
FROM debian:${DEBIAN_VERSION}-slim

ENV UV_HTTP_TIMEOUT=120

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgl1 libglib2.0-0 make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./ /app
WORKDIR /app

COPY --from=uv /uv /uvx /bin/

RUN uv sync

ENTRYPOINT ["uv", "run", "src/cow_amm_trade_envy/main.py"]