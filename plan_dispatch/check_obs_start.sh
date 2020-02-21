#!/bin/bash

it=$1

nohup python check_obs_start.py $it > /dev/null 2>&1 &