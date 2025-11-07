#!/bin/bash

ROOT_DIR=$(echo $(dirname "$0")/..)

export PYTHONPATH=$ROOT_DIR
pytest -v -n1 $ROOT_DIR/tests
