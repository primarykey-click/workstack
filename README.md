## Summary
A massively scalable work queue implemented in Node.js **(currently in alpha state)**


## Example Usage

### Install
```
npm install workstack
```

### Start a Router
```
const { Router } = require("workstack");

var router = new Router({});
router.start();
```

### Start a Worker
```
const { Worker } = require("workstack");

async function work(data)
{   
    console.log(`${new Date()}: working.`);
    console.log("Data received: ", data);
    console.log(`${new Date()}: done.`);
    
    return (new Date()).toString();

}

(   async()=>
    {   
        var worker = new Worker(
            {   pingInterval: 5000,
                queue: "test-queue",
                work: ()=>{console.log("doing work")}
            });
        worker.start();

    }
)()
```

### Send Work
```
const { Producer } = require("workstack");

var producer = new Producer({});
producer.enqueue(
    {   queue: "test-queue",
        command: "execWork", 
        data: {vals: ["1", "2"]}
    });
```

### Authentication
Currently shared key authentication is available.  To enable this, start the tiers (Router, Worker and Producer) with the following additional parameters:

#### Router
```
var router = new Router({authMethod: "sharedKey", authKey: "aequooLohkoa3ar2phee4sheeToxo6"});
```

#### Worker
```
var worker = new Worker(
    {   authKey: "aequooLohkoa3ar2phee4sheeToxo6", 
        pingInterval: 30000, queue: "test-queue",
        work: ()=>{console.log("doing work")}
    });
```

#### Producer
```
var producer = new Producer({authKey: "aequooLohkoa3ar2phee4sheeToxo6"});
```