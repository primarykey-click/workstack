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
        this.worker.work = args.work;
        
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
                var message = JSON.parse(args[1].toString("utf8"));


                switch(message.command)
                {
                    case "execWork":
                        
                        var output = await _this.worker.work(message.data);
                        _this.worker.send([JSON.stringify({command: "workComplete", output: output})]);

                    break;

                }
                //console.log(`Workload: ${JSON.stringify(message)}`);

                //console.log("Workload", workload);
                //var workOutput = await _this.worker.work(message);

            });

    }

}