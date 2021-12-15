#!/bin/bash

while read url; do
   wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies -i $url
   python shotProcessing.py
   done < u_dwn2019.txt
