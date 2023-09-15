#!/bin/bash

# 设置参数
ZOOKEEPER_ADDR="127.0.0.1:9000,127.0.0.1:9001"

start_servers() {
    for port in 8500 8501 8502 8503; do
        java -jar data-server.jar -Dzookeeper.addr="$ZOOKEEPER_ADDR" --server.port="$port" &
    done

    for port in 8200 8201; do
        java -jar meta-server.jar -Dzookeeper.addr="$ZOOKEEPER_ADDR" --server.port="$port" &
    done
}

stop_servers() {
    pids=$(pgrep -f "java -jar")

    if [[ -n "$pids" ]]; then
        echo "Stopping servers..."
        kill "$pids"
    else
        echo "No running servers found."
    fi
}

case "$1" in
    start)
        start_servers
        ;;
    stop)
        stop_servers
        ;;
    *)
        echo "Usage: $0 {start|stop}"
        exit 1
        ;;
esac