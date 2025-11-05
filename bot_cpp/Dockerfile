FROM gcc:11

WORKDIR /app

# Instalar dependências necessárias incluindo Boost
RUN apt-get update && apt-get install -y \
    cmake \
    libzmq3-dev \
    pkg-config \
    git \
    libboost-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar msgpack-c corretamente
RUN git clone https://github.com/msgpack/msgpack-c.git && \
    cd msgpack-c && \
    git checkout cpp_master && \
    cmake -DMSGPACK_BUILD_EXAMPLES=OFF -DMSGPACK_BUILD_TESTS=OFF . && \
    make && make install && \
    cd .. && rm -rf msgpack-c

# Instalar cppzmq (header-only library)
RUN git clone https://github.com/zeromq/cppzmq.git && \
    cd cppzmq && \
    cp *.hpp /usr/local/include/ && \
    cd .. && rm -rf cppzmq

COPY . .

# Compilar o bot C++
RUN g++ -std=c++17 -o bot_cpp bot.cpp -lzmq -lpthread

CMD ["./bot_cpp"]