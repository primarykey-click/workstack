const { Mutex } = require("async-mutex");
const { uuidEmit } = require('uuid-timestamp')
const zmq = require("zeromq/v5-compat");


module.exports = class Worker
{   
    worker = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;
    pingInterval = 30000;
    queue = null;
    mutex = null;
    status = null;


    constructor(args)
    {
        this.worker = zmq.socket("dealer");
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.pingInterval = args.pingInterval ? args.pingInterval : this.pingInterval;
        this.queue = args.queue;
        this.worker.identity = `worker-${uuidEmit()}`;
        this.worker.work = args.work;
        this.mutex = new Mutex();
        
    }

  
    async start()
    {   
        var _this = this;

        console.log(`Worker ${this.worker.identity} ready`);

        this.worker.connect(`tcp://${this.routerAddress}:${this.routerPort}`)

        this.worker.send([JSON.stringify({command: "ready", queue: this.queue})]);
        this.status = "ready";

        this._pingRouter();

        
        this.worker.on("message", async function()
            {   var args = Array.apply(null, arguments);
                var message = JSON.parse(args[1].toString("utf8"));

                console.log(`Received message ${JSON.stringify(JSON.parse(args[1].toString("utf8")))}`);


                switch(message.command)
                {
                    case "confirmReady":
                        
                        _this.worker.send([JSON.stringify({command: "ready", queue: _this.queue})]);

                    break;


                    case "execWork":
                        
                        await _this.mutex.runExclusive(async function()
                            {   
                                _this.status = "working";
                                console.log(JSON.stringify(message));
                                
                                var output = await _this.worker.work(message.data);
                                _this.worker.send([JSON.stringify(
                                    {   command: "workComplete",
                                        queue: message.queue,
                                        workId: message.workId,
                                        output: output
                                    })]);
                                
                                _this.status = "ready";

                            });

                    break;

                }

            });

    }


    _pingRouter()
    {   
        var _this = this;

        setTimeout(async function()
            {   await _this.mutex.runExclusive(function ()
                    {   console.log(`Sending ready ${new Date()} at interval ${_this.pingInterval}`);
                        _this.worker.send([JSON.stringify({command: "ready", queue: _this.queue})]);
                        _this._pingRouter();
                    });           
            }, this.pingInterval);

    }

}