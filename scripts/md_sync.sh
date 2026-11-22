#!/bin/sh
# Copy the local working tree (sources only) to Mystik-Dan's scratch work tree.
cd "$(dirname "$0")/.." && rsync -a --delete --exclude target --exclude data --exclude out --exclude bin --exclude files --exclude starter --exclude .git ./ mystik-dan:buildathon/shopping/work/
