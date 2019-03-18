#!/bin/bash

set -o verbose

swift build -Xcc -I/usr/local/Cellar/unixodbc/2.3.7/include

swift package generate-xcodeproj --xcconfig-overrides SwiftKueryODBCSQL.xcconfig
