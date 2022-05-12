const zmq = require("zeromq/v5-compat");
const uuid = require("uuid").v4;


module.exports = class Worker
{   
    worker = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;


    constructor(args)
    {
        this.worker = zmq.socket("dealer");
        this.routerAddress = args.routerAddress ? args.routerAddress : "127.0.0.1";
        this.routerPort = args.routerPort ? args.routerPort : 5001;
        this.worker.identity = `worker-${uuid()}`;
        this.worker.work = this.work;
        
    }

  
    async start()
    {   
        var _this = this;

        console.log(`Worker ${this.worker.identity} ready`);

        this.worker.connect(`tcp://${this.routerAddress}:${this.routerPort}`)
        this.worker.send([JSON.stringify({command: "ready"})]);
        
        this.worker.on("message", async function()
            {   
                var args = Array.apply(null, arguments);
                var workload = args[1].toString("utf8");

                //console.log("Workload", workload);
                var workOutput = await _this.worker.work(workload);
                _this.worker.send([JSON.stringify({command: "workComplete", workOutput: workOutput})]);

            });

    }


    async work(workload)
    {   console.log(`${new Date()}: working.`);
        console.log("Workload: ", workload);
        await this._sleep(5000);
        console.log(`${new Date()}: done.`);
        
        return {output: (new Date()).toString()}
    }


    _sleep(ms)
    {
        return new Promise((resolve) => 
            {   setTimeout(resolve, ms);
            });

    }

}