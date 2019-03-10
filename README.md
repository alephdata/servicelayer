# servicelayer

[![Build Status](https://travis-ci.org/alephdata/servicelayer.png?branch=master)](https://travis-ci.org/alephdata/servicelayer)

Components of the aleph data toolkit needed to interact with networked services,
such as a storage archive, cache, document conversion and named entity
extraction. This package contains some common configuration components for
all of these services using environment variables. It also contains the protocol
definition files for the gRPC/protobuf interfaces used by aleph.

## archive mechanism

This library provides a configurable method for file storage used by aleph and
memorious. It will store files based on their content hash (SHA1) and allows for
later retrieval of the content.
