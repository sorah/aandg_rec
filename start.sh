#!/bin/bash -xe

if [[ ! -d /app/recorded ]]; then
  mkdir -p /app/recorded
fi

if [[ ! -d /app/log ]]; then
  mkdir -p /app/log
fi

exec ./rec_all.rb
