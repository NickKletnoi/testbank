#!/bin/bash

if ! [[ "18.04 20.04 22.04 22.10" == *"$(lsb_release -rs)"* ]]; then
    echo "Ubuntu $(lsb_release -rs) is not currently supported."
    exit 1
fi

# Install Microsoft ODBC Driver for SQL Server
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Optional: Install sqlcmd and bcp
# sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
# echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc

