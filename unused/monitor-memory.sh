#!/bin/bash

min_memory=$(free | awk '/Mem/ {print $7}')

while true; do
    current_memory=$(free | awk '/Mem/ {print $7}')
    min_memory=$((current_memory < min_memory ? current_memory : min_memory))
    echo "Minimum observed free memory: $min_memory KB"
    sleep 5
done

