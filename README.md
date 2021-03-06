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
xmlaminar defines and implements a clean java and command-line API, 
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
Build using [Maven](http://maven.apache.org/) (i.e., `mvn clean install` from 
top-level project); all dependencies should be available in Maven Central, with 
the exception of a runtime dependency on the Oracle JDBC driver (see below). Top 
level pom file builds all modules. Command-line runnable jar file is generated at: 
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
Command-line syntax is designed to enable pipelineing within a single JVM. Syntax was 
to some degree inspired by the robustness and configurability of the `find` and `rsync` 
commands, and the fluency of the `xrandr` command-line utility.

Command-line structure follows the pattern:
```
java -jar xmlaminar.jar [[--command] [--option [arg]]*]+
```
Each command is configured by 0 or more options (some of which accept arguments), 
and output from each command is "piped" (as SAX events) as input to the subsequent command. 
As a simple example, the following command takes a list of similarly-formatted XML
files on stdin, joins them all together into one large XML file, splits them into 
documents containing 10 records each, and writes each of the resulting documents as gzipped 
files with incrementing file names (/tmp/out.xml-00000.gz,-00001.gz,-00002.gz...) 
```
find . -name '*.xml' -print0 | \
  java -jar xmlaminar.jar --input --from0 --files-from - \
                          --join -a \
                          --split -n 10 \
                          --output -z -b /tmp/out.xml
```
Note that when working with large amounts of XML, the ability to write directly to 
gzipped output files can save a significant amount of disk space and i/o.

##### Input
There are three main types of input that can be specified (including on stdin):

1. Direct (XML content)
2. Indirect (list of Files/URIs containing XML content; compare rsync "files-from" option)
3. Parameters (for commands that support generation of XML content based on input parameters)

Of these three, the first two should be fairly intuitive. The third case is 
designed to be used when XML output is generated as a linear function of a stream of 
input parameters. In a way input type 2 is a special case of input type 3, where the 
"parameters" consist of resource references that define the generated output XML. 

The motivating use case for input type 3 was the `*db-to-xml`-type commands, where a 
parameterized SQL query is used to configure an XMLReader, which dynamically generates 
XML as specified by a stream of input parameters. For instance, for pseudo-SQL configuration
query `SELECT * FROM TABLE WHERE ID IN (<\INTEGER*\>)`, if we pass in delimited stream of a million 
ids (`cat ids.txt | java -jar xmlaminar.jar [...]`), we would expect to receive output 
analogous to:
```xml
<root>
  <record id="[first-id]">
    <col1>val</col1>
    <col2>val</col2>
  </record>
  ...
  <record id="[millionth-id]">
    <col1>val</col1>
    <col2>val</col2>
  </record>
</root>
```
To facilitate JVM-internal pipelining of the generation and consumption of input-type-3
parameter streams, the pipeline automatically pivots according to each command's expected
input type, so that the following command works as one might intuitively hope (generating 
flat XML containing all columns for each row that has been updated in the last half day):
```
echo "0.5" | java -jar xmlaminar.jar \
     --db-to-xml --sql 'SELECT ID FROM TABLE WHERE LAST_UPDATE > SYSDATE - <\DECIMAL\>' \
     --db-to-xml --sql 'SELECT * FROM TABLE WHERE ID IN (<\INTEGER*\>)'
```

##### Configuration options
Most commands (excluding, e.g., the `integrate` command) can be fully configured directly
on the command line. However, to do so would quickly become unwieldy, because of:

1. The profusion of options for some commands
2. The usefulness of chaining (potentially very) many commands together

Also, the inherent composability of pipelines would make it very useful to define a modular
alias for a frequently-used configuration of a command (or pipeline of commands). 

These concerns are addressed by the `config` pseudo-command, whose single XML-configuration-file 
argument bundles command and pipeline configurations together into a single modular 
component that may be used as part of other pipelines or commands -- either on the 
command-line, or nested within other configuration files. 

The below configuration file (invoked `echo 20 | java -jar xmlaminar.jar --config pipeline.xml` 
reads ids for records modified during the past 20 days, and uses them to retrieve 
associated marc records as MARCXML, group them into documents containing 1000 records
each (plus remainder), and write the resulting documents out (gzipped) as out/marc-00000.xml.gz, 
out/marc-00001.xml.gz, etc.

`pipeline.xml`:
```xml
<config:source type="pipeline" xmlns:config="http://library.upenn.edu/xmlaminar/config">
  <config:source type="config">
    incremental.xml
  </config:source>
  <config:filter type="config">
    records.xml
  </config:filter>
  <config:filter type="split">
    chunk-size=1000
  </config:filter>
  <config:filter type="output">
    output-extension=.xml
    output-basename=out/marc
    gzip=true
  </config:filter>
</config:source>
```
`incremental.xml`:
```xml
<config:source type="db-to-xml" xmlns:config="http://library.upenn.edu/xmlaminar/config">
  connection-config-file=connection.properties
  id-field-labels=BIB_ID
  <config:property name="sql">
SELECT DISTINCT BIB_ID
FROM BIB_HISTORY
WHERE ACTION_DATE > SYSDATE - &lt;\DECIMAL\&gt;
ORDER BY 1
  </config:property>
</config:source>
```
`records.xml`:
```xml
<config:source type="marcdb-to-xml" xmlns:config="http://library.upenn.edu/xmlaminar/config">
  connection-config-file=connection.properties
  marc-binary-field-label=RECORD_SEGMENT
  id-field-labels=BIB_ID
  <config:property name="sql">
SELECT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT
FROM PENNDB.BIB_DATA, BIB_MASTER
WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID
  AND BIB_DATA.BIB_ID in (&lt;\INTEGER*\&gt;)
ORDER BY 1,2
  </config:property>
</config:source>
```
Note that properties specified in the config may be contained directly as top-level
text content (which is parsed as a Java properties file) or within a dedicated
`<config:source/>` element. This allows the terseness of a simple properties file to coexist
alongside the richness and explicitness of XML, without sacrificing clarity or 
introducing ambiguity.

### Parallel, fault-tolerant XSLT processing
The `process` command is one of the most powerful xmlaminar commands. Its handful of options
allow for XML streams (consisting of independent records) to be parsed and processed 
in parallel, greatly speeding what has traditionally been a fragile, fundamentally serial task. 

```
Option                       Description
------                       -----------
-r, --split-depth <Integer>  set record element depth (default: 1)
--record-xpath               xpath specifying record id location
-s, --subdivide-on-failure   define behavior on processing failure
-x, --xsl <File>             xsl file defining processing templates
```
By passing a list of files to be processed, or by invoking the `split` command before the `process`
command, chunks of records will be parsed serially, processed in parallel, and output 
corresponding to input order. If the `subdivide-on-failure` flag is specified, a failure 
during processing of a chunk of records will cause the chunk to be recursively subdivided 
(according to the `split-depth` option) and re-processed, isolating the individual problem 
record, logging its id (according to the setting of the `record-xpath` option), and 
allowing the remainder of the records to complete the transformation successfully.

### Output files corresponding to input
In the common use case of transforming a directory full of XML files and writing output
files in an analogous directory structure, xmlaminar may be configured to behave like 
"'rsync --files-from' plus transformation". 

The following command takes all `*.xml` files in the `input/` directory, splits them to
ensure that XSLT processing isn't attempted on chunks of more than 1000 records,
processes the resulting chunks according to `transform.xsl` (subdividing on transformation 
failure and logging failed record ids), joins successfully transformed chunks back 
together (grouping by input systemId), and writes the resulting transformed documents
(gzipped) to a path rooted in the `output/` directory, analogous to the path of the 
corresponding input document (as rooted in the `input/` directory):

```
find input/ -name '*.xml' | \
  java -jar xmlaminar.jar --input -i input/ --files-from - --split -n 1000 \
       --process -s -x transform.xsl --record-xpath '/root/record/@id' 
       --join --output -z -b output/
```

## Extensions
Two main approaches are available for extending the functionality of xmlaminar: 

1. Depend on xmlaminar-cli and package your own additional functionality along
with all dependencies in a single jar file, with the unmodified 
`edu.upenn.library.xmlaminar.cli.Driver` main class, or
2. Package your plugin in a thin jar file (with compile-time dependencies on `xmlaminar-cli`) 
and invoke the stock xmlaminar jar file with a 
`--plugins <colon-separated-paths-to-plugin-jar-files>` initial argument. 

Both approaches allow you to take advantage of the command line arg parsing 
logic and framework, making functionality (including help messages) available
directly on the command-line.  

Several steps should be taken when extending functionality with either of the above 
approaches:

1. Include one or more `edu.upenn.library.xmlaminar.xli.CommandFactory` instances to 
define command-line arguments and certain aspects of command behavior.
   * your `CommandFactory` should include a static initializer block to register 
     itself with the top-level command-line parser:
```java
        class MyCommandFactory extends CommandFactory {
            static {
                registerCommandFactory(new MyCommandFactory());
            }

            @Override
            public String getKey() {
                return "mycommand";
            }
        }
```
2. Include one or more instances of `org.xml.sax.helpers.XMLFilterImpl`, or 
`edu.upenn.library.xmlaminar.parallel.QueueSourceXMLFilter` if you wish to support 
multi-input; optionally extending `edu.upenn.library.xmlaminar.parallel.callback.OutputCallback`
if you wish to support multi-output.
3. If you wish to support the second (i.e., dynamic/runtime) approach to extending 
functionality, you should take the extra step of enumerating desired `CommandFactory`
instances in a special classpath resource file (which you would normally bundl in your 
plugin/extension jar file):
`edu/upenn/library/xmlaminar/cli/load-external-command-factories.ini`. Each 
non-empty, non-whitespace, non-`#`-prefixed line in each such file will be parsed as a 
full-qualified Java class name, and an attempt will be made to load these classes by name 
at runtime. On class load, each named `CommandFactory` should (in its static initializer
block) register an instance of itself with the main command-line parser, making the added
functionality directly available on the command line. 
    * The `xmlaminar-solr` module (which defines a command that unmarshals SAX events
      representing solr input documents into Java `SolrInputDocument` objects and posts
      them in parallel to a solr server via a `ConcurrentUpdateSolrServer`) is an example 
      of this approach. The benefit of this dynamic/runtime approach is that it allows users 
      to swap in specialized modular extensions of functionality without requiring them to
      compile and distribute custom-modified versions of the main code. 
