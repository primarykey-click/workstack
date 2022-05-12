## Summary
A massively scalable work queue implemented in Node.js


## Example Usage

```
const { Worker } = require("../../workstack");


function _sleep(ms)
{
    return new Promise((resolve) => 
        {   setTimeout(resolve, ms);
        });

}


async function work(data)
{   
    console.log(`${new Date()}: working.`);
    console.log("Data received: ", data);
    await _sleep(5000);
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

