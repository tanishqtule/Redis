#!/bin/bash
# Docker health check: healthy when the server answers PING on the given port
# (default 6379). A NOAUTH error counts as healthy too: it shows the server is
# up and serving, and it means the check doesn't need the password. Plain
# bash, so a check every few seconds doesn't pay for starting a JVM.
exec 3<>"/dev/tcp/127.0.0.1/${1:-6379}" || exit 1
printf '*1\r\n$4\r\nPING\r\n' >&3
IFS= read -r -t 2 reply <&3 || exit 1
[[ $reply == +PONG* || $reply == -NOAUTH* ]]
