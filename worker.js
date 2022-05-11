const zmq = require("zeromq/v5-compat");
const uuid = require("uuid").v4;


class Worker
{   
    worker = null;
    workerId = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;


    constructor(args)
    {
        this.worker = zmq.socket("dealer");
        this.workerId = `worker-${uuid()}`;
        this.routerAddress = args.routerAddress ? args.routerAddress : "127.0.0.1";
        this.routerPort = args.routerPort ? args.routerPort : 5001;

    }

  
    async work(workload)
    {   console.log(`${new Date()}: working.`);
        console.log("Workload: ", workload);
        await this._sleep(5000);
        console.log(`${new Date()}: done.`);
        
        return {output: (new Date()).toString()}
    }

       
    async start()
    {   
        var _this = this;

        console.log(`Worker ${workerId} ready`);

        this.worker.identity = workerId;
        this.worker.work = this.work;
        this.worker.connect(`tcp://${routerAddress}:${routerPort}`)
        this.worker.send([JSON.stringify({command: "ready"})]);
        
        this.worker.on("message", async function()
            {   
                var args = Array.apply(null, arguments);
                var workload = args[1].toString("utf8");

                //console.log("Workload", workload);
                var workOutput = await worker.work(workload);
                _this.worker.send([JSON.stringify({command: "workComplete", workOutput: workOutput})]);

            });

    }


    _sleep(ms)
    {
        return new Promise((resolve) => 
            {   setTimeout(resolve, ms);
            });

    }

}