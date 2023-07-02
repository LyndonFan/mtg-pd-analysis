#!/bin/bash
cat .env | grep dbPassword | cut -d '=' -f 2 | pbcopy
