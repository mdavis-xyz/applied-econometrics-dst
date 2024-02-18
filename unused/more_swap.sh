#!/bin/bash
# This script creates a file on linux which is used as swap
# useful when you don't have enough memory
# run with sudo
fallocate -l 8G /swapfile_extend
chmod 600 /swapfile_extend
mkswap /swapfile_extend
swapon /swapfile_extend
