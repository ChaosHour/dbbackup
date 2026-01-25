#!/bin/bash
# Tmux starter with proper UTF-8 and color support

export TERM=xterm-256color
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# Start tmux with UTF-8 support
exec tmux -u "$@"
