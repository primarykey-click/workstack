## Summary
A massively scalable work queue implemented in Node.js **(work in progress - not yet production ready)**


## Example Usage

### Start a Router
```
const { Router } = require("../workstack");

var router = new Router({});
router.start();
```

### Start a Worker
```
const { Worker } = require("../workstack");

async function work(data)
{   
    console.log(`${new Date()}: working.`);
    console.log("Data received: ", data);
    console.log(`${new Date()}: done.`);
    
    return (new Date()).toString();

}

(   async()=>
    {   
        var worker = new Worker({pingInterval: 5000, queue: "test-queue", work: ()=>{console.log("test")}});
        worker.start();

    }
)()
```

### Send Work
```
const { Producer } = require("../workstack");

var producer = new Producer({});
producer.enqueue(JSON.stringify(
    {   queue: "test-queue",
        command: "execWork", 
        data: {vals: ["1", "2"]}
    }));
```

### Authentication
Currently shared key authentication is available.  To enable this please start the tiers (Router, Worker and Producer) with the following additional parameters:

#### Router
```
var router = new Router({authMethod: "sharedKey", authKey: "Quoh4tho3deebih{ei2ohpie8ok{eiceho"});
```

#### Worker
```
var worker = new Worker({authKey: "Quoh4tho3deebih{ei2ohpie8ok{eiceho", 
    pingInterval: 30000, queue: "test-queue", work: ()=>{console.log("test")}});
```

#### Producer
```
var producer = new Producer({authKey: "Quoh4tho3deebih{ei2ohpie8ok{eiceho"});
```