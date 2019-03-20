<p align="center">
    <a href="http://kitura.io/">
        <img src="https://raw.githubusercontent.com/IBM-Swift/Kitura/master/Sources/Kitura/resources/kitura-bird.svg?sanitize=true" height="100" alt="Kitura">
    </a>
</p>




# Swift-Kuery-ODBCSQL
This is very much a work in progress.   I am gradually adding support for all of the required classes to implement the full set of Kuery functionallity.

# Usage

Add dependencies

Add the SwiftQueryODBC package to the dependencies within your application’s Package.swift file. Substitute "x.x.x" with the latest SwiftQueryODBCSQL release.

.package(url: "https://github.com/richardpauljones/SwiftQueryODBCSQL.git", from: "x.x.x")
Add SwiftQueryODBCSQL to your target's dependencies:

.target(name: "example", dependencies: ["SwiftQueryODBCSQL"]),
Import package</br>

import SwiftQueryODBCSQL</br>
Build and test linking</br>
</br>
You must have unixODBC installed on your machine:</br>
</br>
MacOS:</br>
brew install unixodbc</br>
Linux:</br>
apt-get install unixodbc-dev</br>

# Thankyou

To the helpful guys at IBM
* Andrew Lees
* Matt Kilner
* Chris Bailey


## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/IBM-Swift/SwiftKueryPostgreSQL/blob/master/LICENSE.txt)
