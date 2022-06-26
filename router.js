const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
//const zmq = require("zeromq/v5-compat");
const zmq = require("zeromq");


module.exports = class Router
{
    workers = {};
    workPendingStart = {};
    router = null;
    listenPort = 5000;
    listenInterface = "*";
    debug = false;
    dbFile = "db.json";
    db = null;
    authMethod = "none";
    authKey = "auth.key";
    lastWorkerIndex = 0;


    constructor(args)
    {   this.router = new zmq.Router();
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.replyListenPort = args.replyListenPort ? args.replyListenPort : this.replyListenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.debug = args.debug ? args.debug : false;
        this.dbFile = args.dbFile ? args.dbFile : this.dbFile;
        this.authMethod = args.authMethod ? args.authMethod : this.authMethod;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.mutex = new Mutex();

    }


    async start()
    {   
        this.db = new JsonDB(new JsonDbConfig(this.dbFile, false, true, "/"));

        /*try
        {   this.workers = this.db.getData("/workers");
        }
        catch(err)
        {   
            console.log("No workers registered");

        }*/


        await this.router.bind(`tcp://${this.listenInterface}:${this.listenPort}`);

        console.log(`Listening on ${this.listenInterface}:${this.listenPort}`);


        /*for(var workerId of Object.keys(this.workers))
        {   
            delete this.workers[workerId];
            this.db.delete(`/workers/${workerId}`);
            
            console.log(`Sending confirmReady command to ${workerId}`);

            this.router.send([workerId, "", JSON.stringify(
                {   id: uuidEmit(),
                    command: "confirmReady"
                })]);

        }*/


        // Add background thread (setTimeout()) to check for workPendingStart items and put them back into the queue if passed the expiry threshold


        for await (var [id, msg] of this.router)
        {   
            var clientId = id.toString("utf8");
            var message = JSON.parse(msg.toString("utf8"));

            
            switch(this.authMethod)
            {   
                case "sharedKey":

                    if(message.authKey !== this.authKey)
                    {   
                        console.log(`Unauthorized or absent auth key: ignoring message ${JSON.stringify(message)}.`);

                        return;

                    }

                break;

            }

            if(message.command != "ready" || (message.command == "ready" && this.debug))
            {   console.log(`Received message with ID ${JSON.stringify(message.id)} from client ${clientId}`);
            }

            
            switch(message.command)
            {   
                case "working":
                    
                    delete this.workPendingStart[message.workId];

                break;


                case "ready":
                    
                    this.setWorkerReady(clientId, message);

                break;


                case "offline":
                    
                    delete this.workers[clientId];
                    this.db.delete(`/workers/${clientId}`);
                    this.db.save();
                    

                break;


                case "execWork":

                    this.db.push(`/queues/${message.queue}/not-started[]`, 
                        {   received: (new Date()).getTime(),
                            workId: message.id,
                            data: message.data,
                            producerId: clientId
                        });
                    this.db.save();

                    var readyWorkerId = await this.reserveReadyWorker(message);

                    if(readyWorkerId)
                    {   
                        this.workPendingStart[message.id] = 
                        {   workerId: readyWorkerId,
                            pendingSince: (new Date()).getTime()
                        }

                        this.startWork(readyWorkerId, message.queue);

                    }
                    else
                    {   console.log("Ready worker ID: ", readyWorkerId);
                        console.log(`[${new Date()}] No workers ready: queueing message ${message.id}`);

                    }

                    
                    /*for(var workerId of Object.keys(this.workers))
                    {   
                        var worker = this.workers[workerId];

                        if(worker.status == "ready")
                        {   
                            this.workPendingStart[message.id] = 
                            {   workerId: workerId,
                                pendingSince: (new Date()).getTime()
                            }

                            this.startWork(workerId, message.queue);

                            break;

                        }

                    }*/

                    

                break;


                case "workComplete":
                    
                    console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.output)}`);
                    //this.workers[clientId].status = "ready";

                    this.db.push(`/queues/${message.queue}/worked/${message.workId}`,
                        {completed: (new Date()).getTime(), status: "complete", output: message.output}, false);
                    this.db.save();

                    console.log(`Sending message workComplete for message ${JSON.stringify(message.id)} to ${message.producerId}`);
                    this.router.send([message.producerId, {output: message.output}]);

                break;

            }

        }
        
    }


    async reserveReadyWorker(message)
    {   
        var _this = this;
        var readyWorkerId = null;


        await this.mutex.runExclusive(async function()
            {   
                var workerIds = Object.keys(_this.workers);
                var nextWorkerIndex = _this.lastWorkerIndex + 1;
                var timesWrapped = 0;

                if(workerIds.length == 0)
                {   
                    console.log(`No workers online`);
                
                    return null;

                }


                while(true)
                {   
                    if(nextWorkerIndex >= workerIds.length)
                    {   nextWorkerIndex = 0;
                        timesWrapped++;
                    }

                    //console.log(`Next worker index: ${nextWorkerIndex}, times wrapped: ${timesWrapped}`);
                    var nextWorkerId = workerIds[nextWorkerIndex];

                    var worker = _this.workers[nextWorkerId];

                    if(worker.status == "ready")
                    {   
                        console.log(`Reserving worker ${nextWorkerId}`);

                        _this.lastWorkerIndex = nextWorkerIndex;
                        readyWorkerId =  nextWorkerId;

                        _this.workers[readyWorkerId].status = "working";
                        _this.db.push(`/workers/${readyWorkerId}`, _this.workers[readyWorkerId]);
                        _this.db.save();

                        return;

                    }
                    else
                    {   
                        console.log(`Worker ${worker.id} not ready.  Worker: ${JSON.stringify(worker)}`);
                        nextWorkerIndex++;

                        if((nextWorkerIndex > _this.lastWorkerIndex && timesWrapped > 0) || timesWrapped > 1)
                        {
                            return null;

                        }

                    }

                }

            });

        
        return readyWorkerId;

    }


    async startWork(workerId, queue)
    {   
        var _this = this;


        await this.mutex.runExclusive(async function()
            {
                var workItem = null;

                
                try
                {   
                    workItem = _this.db.getData(`/queues/${queue}/not-started[-1]`);

                }
                catch(err)
                {   
                    if(err.message && err.message.match(/Can't find index -1/g))
                    {
                        if(_this.debug)
                        {   console.log(`No work items in queue ${queue}`);
                        }

                    }
                    else
                    {   console.log(err);                       
                    }

                
                    return;

                }


                if(!workItem)
                {
                    console.log(`No work items found`);

                    return;

                }


                console.log(`Assinging work ${workItem.workId} to ${workerId}`);
                
                _this.router.send([workerId, "", JSON.stringify(
                    {   command: "execWork",
                        queue: queue,
                        workId: workItem.workId,
                        data: workItem.data,
                        producerId: workItem.producerId
                    })]);

                var workItemIndex = _this.db.getIndex(`/queues/${queue}/not-started`, workItem.workId, "workId");
                
                _this.db.delete(`/queues/${queue}/not-started[${workItemIndex}]`);
                _this.db.push(`/queues/${queue}/worked/${workItem.workId}`, 
                    {   received: workItem.received,
                        started: (new Date()).getTime(),
                        status: "in-progress",
                        workerId: workerId,
                        producerId: workItem.producerId,
                        data: workItem.data
                    });

            });

    }


    setWorkerReady(clientId, message)
    {   
        if(this.debug)
        {   console.log(`Setting worker status for ${clientId} to ready`);            
        }

        if(!this.workers[clientId])
        {   this.workers[clientId] = {};
        }

        this.workers[clientId].status = "ready";
        this.workers[clientId].lastActivity = (new Date()).getTime();

        this.db.push(`/workers/${clientId}`, this.workers[clientId]);

        this.startWork(clientId, message.queue);

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
        