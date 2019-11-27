#!/bin/bash

set -euo pipefail

for file in "res/final"/*
do
  gsutil cp "$file" gs://vwib_sephora_sde_test/final/
done