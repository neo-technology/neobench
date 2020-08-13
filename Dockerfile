FROM debian:stretch

ARG NEOBENCH_VERSION=dev
ENV NEOBENCH_VERSION=$NEOBENCH_VERSION

COPY out/neobench_${NEOBENCH_VERSION}_linux_amd64 /usr/bin/neobench

ENTRYPOINT ["/usr/bin/neobench"]