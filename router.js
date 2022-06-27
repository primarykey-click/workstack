const PouchDB = require("pouchdb-node");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
//const zmq = require("zeromq/v5-compat");
const zmq = require("zeromq");
//const { socket } = require("zeromq/v5-compat");

PouchDB.plugin(require("pouchdb-find"));


module.exports = class Router
{
    workers = {};
    workPendingStart = {};
    router = null;
    listenPort = 5000;
    listenInterface = "*";
    readyExpiry = 30000;
    cleanupInterval = 60000;
    pendingWorkExpiry = 15000;
    debug = false;
    dbDir = "./data/db";
    db = null;
    cacheFile = "./data/cache/db.json";
    cache = null;
    authMethod = "none";
    authKey = "auth.key";
    lastWorkerIndex = 0;


    constructor(args)
    {   this.router = new zmq.Router();
        //this.events = this.router.events;
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.replyListenPort = args.replyListenPort ? args.replyListenPort : this.replyListenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.readyExpiry = args.readyExpiry ? args.readyExpiry : this.readyExpiry;
        this.cleanupInterval = args.cleanupInterval ? args.cleanupInterval : this.cleanupInterval;
        this.pendingWorkExpiry = args.pendingWorkExpiry ? args.pendingWorkExpiry : this.pendingWorkExpiry;
        this.debug = args.debug ? args.debug : false;
        this.cacheFile = args.cacheFile ? args.cacheFile : (args.dbFile ? args.dbFile : this.cacheFile);
        this.dbDir = args.dbDir ? args.dbDir : this.dbDir;
        this.authMethod = args.authMethod ? args.authMethod : this.authMethod;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.mutex = new Mutex();

    }


    /*async observe()
    {
        for await (var event of this.events)
        {
            console.log(`Event of type ${event.type} fired`);
            console.log("Event:", JSON.stringify(event));

        }
        
    }*/


    async start()
    {   
        this.cache = new JsonDB(new JsonDbConfig(this.cacheFile, false, true, "/"));
        
        this.db = new PouchDB(`${this.dbDir}/workstack`);
        await this.db.createIndex({index: {fields: ["type", "workId"]}});

        await this.router.bind(`tcp://${this.listenInterface}:${this.listenPort}`);
        console.log(`Listening on ${this.listenInterface}:${this.listenPort}`);


        this.cleanup();


        /*for await (var event of this.events)
        {
            console.log(`Event of type ${event.type} fired`);

        }*/


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
            {   console.log(`Received command ${message.command} within message with ID ${JSON.stringify(message.id)} from client ${clientId}`);
            }

            
            switch(message.command)
            {   
                case "working":
                    
                    delete this.workPendingStart[clientId];

                break;


                case "ready":
                    
                    this.setWorkerReady(clientId, message);

                break;


                case "offline":
                    
                    if(this.workers[message.queue] && this.workers[message.queue][clientId])
                    {   delete this.workers[message.queue][clientId];
                    }

                break;


                case "execWork":

                    var received = (new Date()).getTime();

                    this.cache.push(`/queues/${message.queue}/not-started[]`, 
                        {   received: received,
                            workId: message.id,
                            data: message.data,
                            producerId: clientId
                        });
                    this.cache.save();

                    var readyWorkerId = await this.reserveReadyWorker(message);

                    if(readyWorkerId)
                    {   
                        this.workPendingStart[readyWorkerId] = 
                        {   queue: message.queue,
                            workId: message.id,
                            pendingSince: (new Date()).getTime()
                        }

                        this.startWork(readyWorkerId, message.queue);

                    }
                    else
                    {   console.log("Ready worker ID: ", readyWorkerId);
                        console.log(`[${new Date()}] No workers ready: queueing message ${message.id}`);

                    }

                break;


                case "workComplete":
                    
                    console.log(`Work for message ${message.id} completed by ${clientId}.`);

                    //this.cache.push(`/queues/${message.queue}/worked/${message.workId}`,
                    //    {completed: (new Date()).getTime(), status: "complete", output: message.output}, false);
                    //this.cache.save();
                    await this.cache.delete(`/queues/${message.queue}/worked/${message.workId}`);
                    await this.db.put({_id: uuidEmit(), type: "workOutput", workId: message.workId, queue: message.queue, workerId: clientId, output: JSON.parse(message.output)});

                    console.log(`Sending message workComplete for message ${JSON.stringify(message.id)} to ${message.producerId}`);
                    this.router.send([message.producerId, JSON.stringify({id: uuidEmit(), output: JSON.parse(message.output)})]);

                break;


                case "getWorkResult":

                    var workResult = await this.getWorkResult(message.workId);

                    console.log(`Sending work result for work item ${JSON.stringify(message.workId)} to ${clientId}`);
                    this.router.send([clientId, JSON.stringify({id: uuidEmit(), workResult: workResult})]);

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
                if(!_this.workers[message.queue])
                {   _this.workers[message.queue] = {};            
                }

                var workerIds = Object.keys(_this.workers[message.queue]);
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

                    var worker = _this.workers[message.queue][nextWorkerId];

                    if(worker.status == "ready")
                    {   
                        console.log(`Reserving worker ${nextWorkerId}`);

                        _this.lastWorkerIndex = nextWorkerIndex;
                        readyWorkerId =  nextWorkerId;

                        _this.workers[message.queue][readyWorkerId].status = "working";

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
                    workItem = _this.cache.getData(`/queues/${queue}/not-started[-1]`);

                }
                catch(err)
                {   
                    if(err.message && err.message.match(/(Can't find dataPath)|(Can't find index)/g))
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

                var workItemIndex = _this.cache.getIndex(`/queues/${queue}/not-started`, workItem.workId, "workId");
                
                _this.cache.delete(`/queues/${queue}/not-started[${workItemIndex}]`);
                _this.cache.push(`/queues/${queue}/worked/${workItem.workId}`, 
                    {   received: workItem.received,
                        started: (new Date()).getTime(),
                        status: "in-progress",
                        workerId: workerId,
                        producerId: workItem.producerId,
                        data: workItem.data
                    });
                _this.cache.save();

            });

    }


    setWorkerReady(clientId, message)
    {   
        if(this.debug)
        {   console.log(`Setting worker status for ${clientId} to ready`);            
        }

        if(!this.workers[message.queue])
        {   this.workers[message.queue] = {};            
        }

        if(!this.workers[message.queue][clientId])
        {   this.workers[message.queue][clientId] = {};
        }

        this.workers[message.queue][clientId].status = "ready";
        this.workers[message.queue][clientId].lastActivity = (new Date()).getTime();

        this.startWork(clientId, message.queue);

    }


    cleanup()
    {   
        var _this = this;

        
        if(this.debug)
        {   console.log(`[${(new Date()).getTime()}] Cleaning up`);
        }

        
        setTimeout(async function()
            {   
                /* Purge expired workers */

                await _this.mutex.runExclusive(function ()
                    {   
                        for(var queue of Object.keys(_this.workers))
                        {   
                            for(var workerId of Object.keys(_this.workers[queue]))
                            {   
                                var lastActivity = _this.workers[queue][workerId].lastActivity;
                                var now = (new Date()).getTime();
                                
                                if(now - lastActivity >= _this.readyExpiry)
                                {   
                                    console.log(`Purging expired worker ${workerId}`);
                                    delete _this.workers[queue][workerId];

                                }

                            }

                        }

                    });

                
                /* Re-queue orphaned work */
        
                for(var workerId of Object.keys(_this.workPendingStart))
                {
                    var pendingWork = _this.workPendingStart[workerId];
                    var now = (new Date()).getTime();

                    if(now - pendingWork.pendingSince >= _this.pendingWorkExpiry)
                    {   
                        console.log(`Requeuing orphaned work item with ID ${pendingWork.workId} pending since ${(new Date(pendingWork.pendingSince)).toString()}`);
                        
                        await _this.mutex.runExclusive(function ()
                            {   
                                var workItem = _this.cache.get(`/queues/${pendingWork.queue}/worked/${pendingWork.workId}`);
                                
                                _this.cache.push(`/queues/${pendingWork.queue}/not-started[]`, 
                                    {   received: workItem.received,
                                        workId: pendingWork.workId,
                                        data: workItem.data,
                                        producerId: workItem.producerId
                                    });

                                _this.cache.delete(`/queues/${pendingWork.queue}/worked/${pendingWork.workId}`);

                                delete _this.workers[pendingWork.queue][workerId];
                                

                            });

                    }


                }

                _this.cleanup();

            }, _this.cleanupInterval);

    }


    getQueued(queue)
    {
        return this.cache.getData(`/queues/${queue}`);

    }


    async getWorkResult(workId)
    {
        var response = await this.db.find({selector: {type: "workOutput", workId: workId}});
        var workResult = {};
        console.log("Response: ", JSON.stringify(response));

        if(response.docs.length > 0)
        {   workResult = response.docs[0];
        }

        
        return workResult;

    }


    getWorkers(queue)
    {
        if(queue)
        {   
            return this.workers[queue];

        }


        return this.workers;

    }


    getWorker(workerId, queue)
    {
        return this.workers[queue][workerId];

    }

}
        