const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq/v5-compat");


module.exports = class Worker
{   
    worker = null;
    routerAddress = "127.0.0.1";
    routerPort = 5000;
    pingInterval = 30000;
    queue = null;
    mutex = null;
    status = null;
    authKey = null;


    constructor(args)
    {
        this.worker = zmq.socket("dealer");
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.pingInterval = args.pingInterval ? args.pingInterval : this.pingInterval;
        this.authKey = args.authKey ? args.authKey : this.authKey;
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

        this.sendMessage({command: "ready", queue: this.queue});
        this.status = "ready";

        this.pingRouter();

        
        this.worker.on("message", async function(id, msg)
            {   //var args = Array.apply(null, arguments);
                var message = JSON.parse(msg.toString("utf8"));

                console.log(`Received message ${JSON.stringify(JSON.parse(msg.toString("utf8")))}`);


                switch(message.command)
                {
                    case "confirmReady":
                        
                        _this.sendMessage({command: "ready", queue: _this.queue});

                    break;


                    case "execWork":
                        
                        await _this.mutex.runExclusive(async function()
                            {   
                                _this.status = "working";
                                console.log(JSON.stringify(message));
                                
                                var output = await _this.worker.work(message.data);
                                _this.sendMessage(
                                    {   command: "workComplete",
                                        queue: message.queue,
                                        workId: message.workId,
                                        producerId: message.producerId,
                                        output: JSON.stringify(output)
                                    });
                                
                                _this.status = "ready";

                            });

                    break;

                }

            });

    }


    pingRouter()
    {   
        var _this = this;

        setTimeout(async function()
            {   await _this.mutex.runExclusive(function ()
                    {   console.log(`Sending ready ${new Date()} at interval ${_this.pingInterval}`);
                        _this.sendMessage({command: "ready", queue: _this.queue});
                        _this.pingRouter();
                    });           
            }, this.pingInterval);

    }


    sendMessage(message)
    {   
        var modifiedMessage = message;

        if(this.authKey)
        {   modifiedMessage.authKey = this.authKey;
        }

        console.log(`Sending message ${JSON.stringify(message)}`);

        this.worker.send([JSON.stringify(modifiedMessage)]);

    }    

}