#!/bin/bash

spark-submit --master local src/main.py

spark-submit --master local src/test_main.py 