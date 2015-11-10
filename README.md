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

