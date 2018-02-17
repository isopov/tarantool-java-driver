#!/usr/bin/env bash

curl http://download.tarantool.org/tarantool/${TARANTOOL_VERSION}/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

# install https download transport for APT
sudo apt-get -y install apt-transport-https

# append two lines to a list of source repositories
sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_${TARANTOOL_VERSION}.list <<- EOF
deb http://download.tarantool.org/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main
deb-src http://download.tarantool.org/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main
EOF

# install
sudo apt-get update
sudo apt-get -y install tarantool
