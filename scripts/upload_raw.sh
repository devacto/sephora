#!/bin/bash

set -euo pipefail

for file in "res/raw"/*
do
  gsutil cp "$file" gs://vwib_sephora_sde_test/raw/
done