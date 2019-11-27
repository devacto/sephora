#!/bin/bash

set -euo pipefail

gcloud composer environments storage dags import \
  --environment sephora  --location asia-northeast1 \
  --source dags/product.py