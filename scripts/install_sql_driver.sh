#!/bin/bash

# Check if the current Ubuntu version is supported
if ! [[ "18.04 20.04 22.04 22.10" == *"$(lsb_release -rs)"* ]]; then
    echo "Ubuntu $(lsb_release -rs) is not currently supported."
    exit 1
fi

# Add Microsoft package signing key
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

# Add the Microsoft SQL Server repository
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update package lists
sudo apt-get update

# Install the ODBC Driver for SQL Server
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Optional: Install tools for BCP and SQLCMD
# sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
# echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# source ~/.bashrc

# Optional: Install unixODBC development headers
# sudo apt-get install -y unixodbc-dev
