# URLFileProcessor

This project fetch URLs from files/directory and make bulk HTTP get calls.

## Prerequisites

* JDK 8
* Maven

## How to run

Clone the project
```
git clone https://github.com/prathameshjagtap/URLFileProcessor.git
```

Clone the project
```
cd URLFileProcessor
```

Build project
```
mvn install
```

Copy input file into directory. Replace <INPUT_FILE> with location of your *.zip file or directory.
```
cp <INPUT_FILE> .
```

Run the project. Provide number of cores you want to provide. If parameters are not passed, by default it will use all the available CPUs and will look for inputData.zip 
```
java -jar target/URLFileProcessor-1.0-shaded.jar <NO_OF_CORES> inputData.zip
```

## Pipeline Architecture

![Pipeline Architecture](https://pratham-public-bucket.s3.amazonaws.com/URLFileProcessor.png)

### URLFileProcessor
Main class responsible for Driving the URL File processing.

### Progress Report
Maintains status of job. This class have methods to keep track of success and failure stats.

### AsyncFileReader
This worker thread is responsible to read lines from files and add work block to the Blocking Queue.

### FileManager
FileManager manages the files and its blocks in progress. Currently it is using simple algorithm to allot a block to client every time getFileBlock is called. Also manages if the files are processed and are ready to mark for completion.

### HttpClientManager
Manages Http Connection pool for bulk Http Requests. This class continuously tunes the connection pool to allot more connections to most commonly used URL.

### HttpGetBlockProcessor
Block processor worker thread responsible to making Http Get calls for each URL in a block. This class works of a BlockingQueue to pull its work.