#!/usr/bin/env bash
set -euo pipefail
cd /home/admin/.openclaw/workspace/projects/ch-stock
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
pyenv local 3.11.9
source .venv/bin/activate
exec streamlit run app.py --server.headless true --server.address 127.0.0.1 --server.port 18501
