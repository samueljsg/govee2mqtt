#!/bin/sh
# This script updates the version number for
# the addon based on TAG_NAME if specified, otherwise preserves addon/config.yaml
if [ -n "$TAG_NAME" ]; then
  sed -i "s/version:.*/version: \"$TAG_NAME\"/" addon/config.yaml
fi
