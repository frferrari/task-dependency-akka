# Service Deployment Technical Assessment

**Please read the PDF found in this repo** for the details of this assessment

To run the code, you could package or simply issue a `run` command from IntelliJ for example

## API

The API is served from the port 8080

### POST /deploy

It deploys a service based on the provided JSON payload, below is a payload example :

[
    {
        "serviceName": "A",
        "entryPoint": true,
        "replicas": 1,
        "dependencies": ["B", "C", "E"]
    },
        {
        "serviceName": "B",
        "entryPoint": false,
        "replicas": 1,
        "dependencies": ["C", "E"]
    },
    {
        "serviceName": "C",
        "entryPoint": false,
        "replicas": 2,
        "dependencies": ["D"]
    },
    {
        "serviceName": "D",
        "entryPoint": false,
        "replicas": 2,
        "dependencies": []
    },
    {
        "serviceName": "E",
        "entryPoint": false,
        "replicas": 2,
        "dependencies": []
    }
]

### GET /check

It checks if the system is healthy or not
