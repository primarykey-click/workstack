const zmq = require("zeromq/v5-compat");
const uuid = require("uuid").v4;


function _sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms); 
    });
  }
  
  
async function work(workload)
{   console.log(`${new Date()}: working.`);
    console.log("Workload: ", workload);
    await _sleep(5000);
    console.log(`${new Date()}: done.`);
    
    return {output: (new Date()).toString()}
}


(   
    async()=>
    {   
        var worker = zmq.socket("dealer");
        var workerId = `worker-${uuid()}`;

        console.log(`Worker ${workerId} ready`);

        worker.identity = workerId;
        worker.work = work;
        worker.connect("tcp://127.0.0.1:5001")
        worker.send([JSON.stringify({command: "ready"})]);
        
        worker.on("message", async function()
            {   
                var args = Array.apply(null, arguments);
                var workload = args[1].toString("utf8");

                //console.log("Workload", workload);
                var workOutput = await worker.work(workload);
                worker.send([JSON.stringify({command: "workComplete", workOutput: workOutput})]);

            });

    }

)()