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
        var worker = new Worker({work: work});
        worker.start();

    }
)()

```

### Send Work
```
const uuid = require("uuid").v4;
const { Producer } = require("../workstack");

var producer = new Producer({});
producer.enqueue(JSON.stringify(
    {   queue: "test-queue",
        id: uuid(),
        command: "execWork", 
        data: {vals: ["1", "2"]}
    }));

```