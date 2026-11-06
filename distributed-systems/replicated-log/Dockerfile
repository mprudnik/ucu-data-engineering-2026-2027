# One image for both roles. The role is chosen by the command: `master` or `secondary`.

FROM rust:1-slim-trixie AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin replicated-log

FROM debian:trixie-slim
COPY --from=build /app/target/release/replicated-log /usr/local/bin/replicated-log
ENTRYPOINT ["replicated-log"]
