#!/bin/bash

set -ex

docker buildx build --platform linux/amd64 --provenance=false -t rg.fr-par.scw.cloud/funcscwriseriscvrunnerappqdvknz9s/gh-webhook:latest --load --push .
