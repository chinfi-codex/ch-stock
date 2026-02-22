#!/usr/bin/env bash
set -euo pipefail
cd /home/admin/.openclaw/workspace/projects/ch-stock
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local 3.11.9
source .venv/bin/activate
exec python review_scheduler.py --run-once --date "$(date +%F)" --skip-weekend