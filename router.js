const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
const zmq = require("zeromq/v5-compat");


module.exports = class Router
{
    workers = {};
    router = null;
    listenPort = 5001;
    listenInterface = "*";
    dbFile = "db.json";
    db = null;


    constructor(args)
    {   this.router = zmq.socket("router");
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.dbFile = args.dbFile ? args.dbFile : this.dbFile;
        this.mutex = new Mutex();

    }


    start()
    {   
        var _this = this;
        this.db = new JsonDB(new JsonDbConfig(this.dbFile, true, true, "/"));

        try
        {   this.workers = this.db.getData("/workers");
        }
        catch(err)
        {   
            console.log("No workers registered");

        }


        this.router.bind(`tcp://${this.listenInterface}:${this.listenPort}`, 
            async function(err)
            {   
                if(err)
                {   throw(err);            
                }

                
                console.log(`Listening on ${_this.listenInterface}:${_this.listenPort}`);


                for(var workerId of Object.keys(_this.workers))
                {   
                    _this.workers[workerId].status = "pending";
                    _this.db.push(`/workers/${workerId}`, _this.workers[workerId]);
                    
                    console.log(`Sending confirmReady command to ${workerId}`);

                    _this.router.send([workerId, "", JSON.stringify(
                        {   command: "confirmReady"
                        })]);
        
                }
                
                
                _this.router.on("message", async function()
                {   
                    var args = Array.apply(null, arguments);
                    var clientId = args[0].toString("utf8");
                    var message = JSON.parse(args[1].toString("utf8"));
                    console.log(`Received message ${JSON.stringify(message)} from client ${clientId}`);
                    
                    switch(message.command)
                    {   
                        case "ready":
                            
                            if(!_this.workers[clientId])
                            {   _this.workers[clientId] = {};
                            }

                            _this.workers[clientId].status = "ready";
                            _this.workers[clientId].lastActivity = (new Date()).getTime();

                            _this.db.push(`/workers/${clientId}`, _this.workers[clientId]);

                            _this.startWork(clientId, message.queue);
                            

                        break;


                        case "execWork":
                            
                            _this.db.push(`/queues/${message.queue}/not-started[]`, 
                                {   received: (new Date()).getTime(),
                                    workId: message.id,
                                    data: message.data
                                });

                            for(var workerId of Object.keys(_this.workers))
                            {   
                                var worker = _this.workers[workerId];

                                if(worker.status == "ready")
                                {   
                                    await _this.startWork(workerId, message.queue);

                                    break;

                                }

                            }

                        break;


                        case "workComplete":
                            
                            console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.output)}`);
                            _this.workers[clientId].status = "ready";

                            _this.db.push(`/queues/${message.queue}/worked/${message.workId}`,
                                {completed: (new Date()).getTime(), status: "complete", output: message.output}, false);

                        break;

                    }

                })

            });        
    }


    async startWork(workerId, queue)
    {   
        var _this = this;


        await this.mutex.runExclusive(async function()
            {
                var workItem = null;

                console.log(queue);

                                            
                try
                {   
                    workItem = _this.db.getData(`/queues/${queue}/not-started[-1]`);

                }
                catch(err)
                {   
                    console.log(err);
                    //console.log(`No work items`);
                
                    return;

                }


                if(!workItem)
                {
                    console.log(`No work items found: ${JSON.stringify(workItems)}`);

                    return;

                }


                console.log("Work item: ", JSON.stringify(workItem));
                console.log(`Assinging work ${workItem.id} to ${workerId}`);
                
                _this.router.send([workerId, "", JSON.stringify(
                    {   command: "execWork",
                        queue: queue,
                        workId: workItem.workId,
                        data: workItem.data
                    })]);

                var workItemIndex = _this.db.getIndex(`/queues/${queue}/not-started`, workItem.workId, "workId");
                
                _this.db.delete(`/queues/${queue}/not-started[${workItemIndex}]`);
                _this.db.push(`/queues/${queue}/worked/${workItem.workId}`, 
                    {   received: workItem.received,
                        started: (new Date()).getTime(),
                        status: "in-progress"
                    });

                    _this.workers[workerId].status = "working";
                    _this.db.push(`/workers/${workerId}`, _this.workers[workerId]);

            });

    }


    getQueued(queue)
    {
        return this.db.getData(`/queues/${queue}`);

    }


    getWorkStatus(queue, workId)
    {
        return this.db.getData(`/queues/${queue}/${workId}`);

    }


    getWorkers()
    {
        return this.workers;

    }


    getWorker(workerId)
    {
        return this.workers[workerId];

    }

}
        