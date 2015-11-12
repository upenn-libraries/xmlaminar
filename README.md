# xmlaminar
Utilities for working with streaming XML pipelines


## Background and motivation
XML is a widely used format for information serialization, transfer,
and processing.  Unfortunately, because of its document-centric nature,
XML is unnecessarily inconvenient to work with in contexts where content
consists of multiple independent records, and where streaming and/or
fault-tolerant processing are desired.  In practice, records are usually
grouped together arbitrarily into documents for transfer and processing;
this approach often necessitates workarounds to approximate streaming and
fault-tolerance, and pointlessly increases the complexity and inefficiency
of many workflows involving XML.


## Functionality
xml-utils defines and implements a clean java and command-line API, 
providing a flexible level of abstraction for managing the document-based 
nature of XML representations of data.  Functionality enabled includes:

1. Joining multiple XML streams together
2. Splitting XML streams into smaller chunks
3. Fault-tolerant, order-preserving parallel processing of arbitrarily 
   large XML streams (taking advantage of multiple cores to speed
   processing)
4. Streaming XML representation of information from databases (including 
   MARC->MARCXML)
5. Integration of multiple corresponding XML streams into a single 
   hierarchical XML stream
6. Configurable modules for handling XML output (write to file, stdout,
   POST to URL, etc.)

## Design considerations
Main design considerations include:

1. Transparency, accessibility, and flexibility of configuration and use 
2. Ease of extensibility (for supporting new functionality)
3. Emphasis on efficiency (memory and CPU) and support for streaming use
   cases

## Build and use
### Building
Build using maven (i.e., `mvn clean install` from top-level project); 
all dependencies should be available in Maven Central, with the exception
of a runtime dependency on the Oracle JDBC driver (see below). Top level 
pom file builds all modules. Command-line runnable jar file is generated at: 
`cli/dist/xmlaminar-cli-[version]-jar-with-dependencies.jar`

#### JDBC driver dependency
For local/historical reasons, there is an Oracle/JDBC bias in the database-oriented
portion of the codebase. This dependency should be refactored out, but for now
the dependency is referred to in `core/pom.xml` as `com.oracle.ojdbc4@10.2.0.5`
Version 10.2.0.5 is used because it is the most recent driver that maintains 
compatibility with legacy Oracle database versions (8.x?).  This dependency must 
be fetched manually (currently@2015-11-12 [from Oracle](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-10201-088211.html), 
and added to local maven repo according to instructions [from Maven](http://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html), i.e.:
```
mvn install:install-file -Dfile=<path-to-file> -DgroupId=com.oracle \
        -DartifactId=ojdbc4 -Dversion=10.2.0.5 -Dpackaging=jar
```

### Use
#### Command-line help
Referring to the jar-with-dependencies as `xmlaminar.jar`, top-level cli 
help may be accessed via:
```
[user@host]$ java -jar xmlaminar.jar --help
For help with a specific command: 
        --command --help
available commands: 
        [process, integrate, input, marcdb-to-xml, pipeline, tee, config, 
        split, join, output, db-to-xml]
```
As indicated in the output for the top-level `--help` command, many (though
not all) of the `available commands` have their sub-options documented and
accessible via `java -jar xmlaminar.jar --input --help`, e.g.:
```
[user@host]$ java -jar xmlaminar.jar --input --help
Option                              Description
------                              -----------
-0, --from0                         indirect input file null-delimited
-d, --input-delimiter               directly specify input delimiter
                                      (default:
                                    )
--files-from <File: '-' for stdin>  indirect input
-h, --help                          show help
-i, --input <File: '-' for stdin>   input; default to stdin if no --files-from,
                                      otherwise CWD
-v, --verbose                       be more verbose
```

#### Command-line syntax
Command-line syntax is designed to enable pipelineing wihtin a single JVM. Syntax was 
to some degree inspired by the robustness and configurability of the `find` and `rsync` 
commands, and the fluency of the `xrandr` command.

Commind-line structure follows the pattern:
```
java -jar xmlaminar.jar [[--command] [--subcommand [arg]]*]+
```
Each command is configured by 0 or more subcommands, and output from each command is "piped" 
(as SAX events) as input to the subsequent command. 
