#!/bin/bash

set -euo pipefail

export GOOGLE_APPLICATION_CREDENTIALS=sephora-sde-test-8412814137e3.json
export FLASK_APP=app.py
export FLASK_ENV=production

sephora/bin/flask run