const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq/v5-compat");


module.exports = class Worker
{   
    worker = null;
    routerAddress = "127.0.0.1";
    routerPort = 5000;
    pingInterval = 30000;
    debug = false;
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
        this.debug = args.debug ? args.debug : false;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.queue = args.queue;
        this.worker.identity = `worker-${args.workerId ? args.workerId : uuidEmit()}`;
        this.worker.work = args.work;
        this.mutex = new Mutex();
        
    }

  
    async start()
    {   
        var _this = this;

        console.log(`Worker ${this.worker.identity} ready`);

        this.worker.connect(`tcp://${this.routerAddress}:${this.routerPort}`)

        this.sendReady();

        this.pingRouter();

        
        this.worker.on("message", async function(id, msg)
            {   
                var message = JSON.parse(msg.toString("utf8"));

                console.log(`Received message with ID ${JSON.stringify(JSON.parse(msg.toString("utf8")).workId)} and command ${message.command}`);


                switch(message.command)
                {
                    case "execWork":
                        
                        await _this.mutex.runExclusive(async function()
                            {   
                                _this.status = "working";
                                _this.sendMessage({command: "working"});
                                
                                var output = await _this.worker.work(message.data);
                                _this.sendMessage(
                                    {   command: "workComplete",
                                        queue: message.queue,
                                        workId: message.workId,
                                        producerId: message.producerId,
                                        output: JSON.stringify(output)
                                    });
                                
                                _this.sendReady();

                            });

                    break;

                }

            });


        process.on("SIGINT", function(){_this.exitClean();});
        process.on("SIGTERM", function(){_this.exitClean();});

    }


    exitClean()
    {   
        this.sendMessage({command: "offline", queue: this.queue});
        this.worker.close();
        process.exit();

    }


    pingRouter()
    {   
        var _this = this;

        setTimeout(async function()
            {   
                await _this.mutex.runExclusive(function ()
                    {   _this.sendReady();
                    });
                
                _this.pingRouter();

            }, this.pingInterval);

    }


    sendMessage(message)
    {   
        var modifiedMessage = message;

        if(this.authKey)
        {   modifiedMessage.authKey = this.authKey;
        }

        if(!message.id)
        {   message.id = uuidEmit();            
        }

        if(this.debug)
        {   console.log(`Sending message ${JSON.stringify(message)}`);
        }
        else
        {   
            if(message.command != "ready" || (message.command == "ready" && this.debug))
            {   console.log(`Sending message with ID ${message.id} and command "${message.command}"`);            
            }

        }

        this.worker.send([JSON.stringify(modifiedMessage)]);

    }


    sendReady()
    {   
        if(this.debug)
        {   console.log(`[${(new Date()).toString()}] Sending ready`);            
        }

        this.status = "ready";
        this.sendMessage({command: "ready", queue: this.queue});

    }

}