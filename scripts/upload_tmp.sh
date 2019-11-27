#!/bin/bash

set -euo pipefail

for file in "res/tmp"/*
do
  gsutil cp "$file" gs://vwib_sephora_sde_test/tmp/
done