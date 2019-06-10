# URLFileProcessor

This project fetch URLs from files/directory and make bulk HTTP get calls.

### Prerequisites

* JDK 8
* Maven

### How to run

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
maven install
```

Copy input file into directory
```
cp <INPUT_FILE> .
```

Run the project
```
java -jar target/URLFileProcessor-1.0-shaded.jar 4 inputData.zip
```

