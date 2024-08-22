#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
EXTENSIONS_RELATIVE_PATH="./extensions.list"

EXTENSIONS_PATH=$(realpath $SCRIPT_DIR/$EXTENSIONS_RELATIVE_PATH)

export DONT_PROMPT_WSL_INSTALL=true
grep -v '^#' $EXTENSIONS_PATH | xargs -L 1 code --install-extension
