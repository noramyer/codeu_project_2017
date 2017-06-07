
# CODEU CHAT SERVER | README

## DISCLAIMER

CODEU is a program created by Google to develop the skills of future software
engineers. This project is not an offical Google Product. This project is a
playground for those looking to develop their coding and software engineering
skills.

## DEPENDENCIES

  1. Must have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) Installed
  Check Java version with:
     ```
       $ java -version
       ```
  1. Install the Google Cloud SDK [here](https://cloud.google.com/sdk/)
  
  1. Make sure it is correctly in your path before proceeding to run the server

## ENVIRONMENT

All instructions here are relative to a LINUX environment. There will be some
differences if you are working on a non-LINUX system. We will not support any
other development environment.

This project was built using JAVA 8. It is recommended that you install
JAVA&nbsp;8 when working with this project. 

In order for sentiment analysis to work, the Google Cloud SDK must be installed 
on the server machine. Additionally the the cloud SDK must have permission to access
the natural language API. To do this a service account key must be provided. Finally, 
after the SDK has the necessary permissions, an environment variable:
`GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json` 
must be created. 

More Info can be found [here](https://cloud.google.com/natural-language/docs/getting-started)


## GETTING STARTED

  1. To build the project:
       ```
       $ sh clean.sh
       $ sh make.sh
       ```

  1. To test the project:
       ```
       $ sh test.sh
       ```

  1. To run the project you will need to run both the client and the server. Run
     the following two commands in separate shells:

       ```
       $ sh run_server.sh <team_id> <team_secret> <port> <persistent-dir>
       $ sh run_simple_gui_client2.sh <host> <port>
       ```
  1. To observe the user sentiment scores, in a seperate shell run: 
       ```
       $ sh run_client.sh <host> <port>
       >> u-list-all
       ```

     You must specify the following startup arguments for `run_server.sh:
     + `<team_id>` and `<team_secret>`: a numeric id for your team, and a secret
       code, which are used to authenticate your server with the Relay server.
       You can specify any integer value for `<team_id>`, and a value expressed
       in hexadecimal format (using numbers `0-9` and letters in the range
       `A-F`) for `<team_secret>` when you launch the server in your local setup
       since it will not connect to the Relay server.
     + `<port>`: the TCP port that your Server will listen on for connections
       from the Client. You can use any value between 1024 and 65535, as long as
       there is no other service currently listening on that port in your
       system. The server will return an error:

         ```
         java.net.BindException: Address already in use (Bind failed)
         ```

       if the port is already in use.
     + `<persistent-dir>`: the path where you want the server to save data between
       runs.

     The startup arguments for `run_client.sh` are the following:
     + `<host>`: the hostname or IP address of the computer on which the server
       is listening. If you are running server and client on the same computer,
       you can use `localhost` here.
     + `<port>`: the port on which your server is listening. Must be the same
       port number you have specified when you launched `run_server.sh`.

All running images write informational and exceptional events to log files.
The default setting for log messages is "INFO". You may change this to get
more or fewer messages, and you are encouraged to add more LOG statements
to the code. The logging is implemented in `codeu.chat.util.Logger.java`,
which is built on top of `java.util.logging.Logger`, which you can refer to
for more information.

In addition to your team's client and server, the project also includes a
Relay Server and a script that runs it (`run_relay.sh`).
This is not needed to get started with the project.


## Finding your way around the project

All the source files (except test-related source files) are in
`./src/codeu/chat`.  The test source files are in `./test/codeu/chat`. If you
use the supplied scripts to build the project, the `.class` files will be placed
in `./bin`. There is a `./third_party` directory that holds the jar files for
JUnit (a Java testing framework). Your environment may or may not already have
this installed. The supplied scripts use the version in `./third_party`.

Finally, there are some high-level design documents in the project Wiki. Please
review them as they can help you find your way around the sources.



## Source Directories

The major project components have been separated into their own packages. The
main packages/directories under `src/codeu/chat` are:

### codeu.chat.client

Classes for building the three clients (`codeu.chat.ClientMain`,
`codeu.chat.SimpleGuiClientMain`, and `codeu.chat.SimpleGuiClientMain2`).

### codeu.chat.server

Classes for building the server (`codeu.chat.ServerMain`).

### codeu.chat.relay

Classes for building the Relay Server (`codeu.chat.RelayMain`). The Relay Server
is not needed to get started.

### codeu.chat.common

Classes that are shared by the clients and servers.

### codeu.chat.util

Some basic infrastructure classes used throughout the project.
