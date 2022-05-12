## Summary
A massively scalable work queue implemented in Node.js **(work in progress)**


## Example Usage

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

