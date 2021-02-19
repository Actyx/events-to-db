# 1: Build the binary
FROM rust:1.49 as build
WORKDIR /usr/src

# 1a: Prepare for static linking
RUN apt-get update && \
  apt-get dist-upgrade -y && \
  apt-get install -y musl-tools unzip && \
  rustup target add x86_64-unknown-linux-musl

# Compile OpenSSL so we can do a MUSL build (see https://qiita.com/liubin/items/6c94f0b61f746c08b74c)
RUN curl -OL https://github.com/openssl/openssl/archive/OpenSSL_1_1_1i.zip && \
  unzip OpenSSL_1_1_1i.zip && \
  cd openssl-OpenSSL_1_1_1i && \
  CC="musl-gcc -fPIE -pie" ./Configure no-shared no-async no-engine --prefix=/musl --openssldir=/musl/ssl -DOPENSSL_NO_SECURE_MEMORY linux-x86_64 && \
  make depend && \
  make -j8 && \
  make install_sw

# 1b: Download and compile Rust dependencies (and store as a separate Docker layer)
RUN USER=root cargo new app
WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock ./
RUN PKG_CONFIG_ALLOW_CROSS=1 OPENSSL_STATIC=true OPENSSL_DIR=/musl cargo build --release --target x86_64-unknown-linux-musl

# 1c: Build the binary using the actual source code
COPY src ./src
RUN touch src/* && \
  PKG_CONFIG_ALLOW_CROSS=1 OPENSSL_STATIC=true OPENSSL_DIR=/musl cargo build --release --target x86_64-unknown-linux-musl

# 2: Copy the binary  to an empty Docker image
FROM scratch
COPY --from=build /usr/src/app/target/x86_64-unknown-linux-musl/release/events-to-db .
USER 1000
ENTRYPOINT ["./events-to-db"]
