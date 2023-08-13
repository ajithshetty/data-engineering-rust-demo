#!/bin/bash
file="jars.txt"
while read line; do
  wget -P /opt/spark/jars/ $line
done < "${file}"