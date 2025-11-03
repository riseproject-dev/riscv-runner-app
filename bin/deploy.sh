#!/bin/bash

set -e

./bin/build.sh

export SCW_SECRET_KEY="op://RISE/rise-riscv-runner api key/password"
export GITHUB_WEBHOOK_SECRET="op://RISE/rise-riscv-runner gh-webhook secret/password"
export GITHUB_APP_PRIVATE_KEY="op://RISE/rise-riscv-runner private key/private key"

op run -- serverless deploy --verbose

