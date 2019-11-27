#!/bin/bash

set -euo pipefail

curl -i -X POST "http://localhost:5000/products?id=9&name=%27banana%20lipstick%27&category_id=4&external_id=123&type=%27product%27"