# StreamPatrol

StreamPatrol is a stream monitoring solution built using Apache Spark, Delta Lake, and Kafka. It provides functionalities for generating random data streams, ingesting data from Kafka, storing data in Delta Lake, and reading from Delta tables.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Streaming Demo](#streaming-demo)
  - [Kafka to Delta Delivery](#kafka-to-delta-delivery)
  - [Delta Table Reader](#delta-table-reader)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Random Data Stream Generation**: Generate random strings and stream them using Spark.
- **Kafka Integration**: Read and write data streams to and from Kafka.
- **Delta Lake Storage**: Store streaming data in Delta Lake for efficient querying and reliability.
- **Delta Table Reading**: Read and display data from Delta tables.

## Installation

1. **Clone the repository**:
   ```sh
   git clone <repository_url>
   cd streamPatrol
