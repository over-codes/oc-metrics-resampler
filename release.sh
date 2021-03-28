#!/bin/bash

set -e

rustup target add x86_64-unknown-linux-musl

cargo build --target x86_64-unknown-linux-musl --release

TARGET=$(pwd)/target/x86_64-unknown-linux-musl/release/oc-metrics-resampler

BUILDDIR=$(mktemp -d)

pushd $BUILDDIR

cp $TARGET ./exe

cat <<EOF > Dockerfile
FROM scratch

ADD exe /exe

ENV LISTEN="[::]:50051"
EXPOSE 50051
CMD ["/exe"]
EOF

docker build --tag aura:5000/oc-metrics-resampler .
