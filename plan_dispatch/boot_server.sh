#!/bin/bash

#path=$1
script=$1

#cd $path
nohup python $script > /dev/null 2>&1 &