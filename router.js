const crypto = require("crypto");
const express = require("express");
const fs = require("fs");
const PouchDB = require("pouchdb-node");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq");
const { fstat } = require("fs");
const WorkStackCrypto = require(`${__dirname}/local_modules/WorkStackCrypto`);

PouchDB.plugin(require("pouchdb-find"));


module.exports = class Router
{
    workers = {};
    producers = {};
    workPendingStart = {};
    router = null;
    listenPort = 5000;
    managementPort = 5001;
    listenInterface = "*";
    readyExpiry = 30000;
    cleanupInterval = 60000;
    pendingWorkExpiry = 600000;  //15000;
    encrypt = false;
    keyLength = 2048;
    keyPair = null;
    encryptAlgorithm = "aes-256-cbc";
    debug = false;
    dbDir = "./data/db";
    db = null;
    cacheFile = "./data/cache/db.json";
    cache = null;
    authMethod = "none";
    authKey = "auth.key";
    lastWorkerIndex = 0;


    constructor(args)
    {   
        this.router = new zmq.Router({sendHighWaterMark: 0});
        //this.events = this.router.events;
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.managementPort = args.managementPort ? args.managementPort : this.managementPort;
        this.replyListenPort = args.replyListenPort ? args.replyListenPort : this.replyListenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.readyExpiry = args.readyExpiry ? args.readyExpiry : this.readyExpiry;
        this.cleanupInterval = args.cleanupInterval ? args.cleanupInterval : this.cleanupInterval;
        this.pendingWorkExpiry = args.pendingWorkExpiry ? args.pendingWorkExpiry : this.pendingWorkExpiry;
        this.encrypt = args.encrypt ? args.encrypt : this.encrypt;
        this.encryptAlgorithm = args.encryptAlgorithm ? args.encryptAlgorithm : this.encryptAlgorithm;
        this.keyLength = args.keyLength ? args.keyLength : this.keyLength;
        this.debug = args.debug ? args.debug : false;
        this.cacheFile = args.cacheFile ? args.cacheFile : (args.dbFile ? args.dbFile : this.cacheFile);
        this.dbDir = args.dbDir ? args.dbDir : this.dbDir;
        this.authMethod = args.authMethod ? args.authMethod : this.authMethod;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.mutex = new Mutex();

        if(this.encrypt)
        {
            this.keyPair = crypto.generateKeyPairSync("rsa",
                {   modulusLength: this.keyLength,
                    publicKeyEncoding:
                    {   type: "spki",
                        format: "pem"
                    },
                    privateKeyEncoding:
                    {   type: "pkcs8",
                        format: "pem"
                    }
                });

        }

    }


    /*async observe()
    {
        for await (var event of this.events)
        {
            console.log(`Event of type ${event.type} fired`);
            console.log("Event:", JSON.stringify(event));

        }
        
    }*/


    startManagementListener()
    {   
        var _this = this;

        var app = express();
        app.get("/workers", async function(req, res, next)
            {   
                try
                {   
                    var workers = _this.getWorkers(req.query.queue);
                    res.json(workers);

                }
                catch(err)
                {
                    next(err);

                }
                                
            });


        app.get("/diagnostics", async function(req, res, next)
            {   
                try
                {   
                    var diagnostics = _this.getDiagnostics();
                    res.json(diagnostics);

                }
                catch(err)
                {
                    next(err);

                }
                                
            });


        app.listen(this.managementPort);
        console.log(`Express started on port ${this.managementPort}`);

    }


    async start()
    {   
        this.startManagementListener();


        this.cache = new JsonDB(new JsonDbConfig(this.cacheFile, false, true, "/"));
        
        if(!fs.existsSync(`${this.dbDir}/workstack`))
        {   fs.mkdirSync(`${this.dbDir}/workstack`, {recursive: true});            
        }

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
            var rawMessage = JSON.parse(msg.toString("utf8"));
            var message = null;
            
            try
            {   message = rawMessage.encrypted ? JSON.parse(WorkStackCrypto.decryptMessage(rawMessage, this.keyPair.privateKey)) : rawMessage;
            }
            catch(err)
            {
                console.log(`Error decrypting message.  Key length: ${this.keyLength}`);
                console.log(`Message: `, JSON.stringify(rawMessage, null, "\t"));
                
                throw(err);

            }

            
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

            /*if(message.command != "ready" || (message.command == "ready" && this.debug))
            {   console.log(`Received command ${message.command} within message with ID ${JSON.stringify(message.id)} from client ${clientId}`);
            }*/
            console.log(`Received command ${message.command} within message with ID ${JSON.stringify(message.id)} from client ${clientId}`);

            
            switch(message.command)
            {   
                case "working":
                    
                    delete this.workPendingStart[clientId];

                break;


                case "ready":
                    
                    await this.setWorkerReady(clientId, message);

                break;


                case "online":

                    await this.setWorkerOnline(clientId, message);

                break;


                /*case "setKey":
                
                    this.producers[clientId].publicKey = message.publicKey;

                break;*/


                case "setGetKey":

                    if(!this.producers[clientId])
                    {   this.producers[clientId] = {};                        
                    }

                    this.producers[clientId].publicKey = message.publicKey;
                    await this.sendMessage(clientId, {command: "setKey", publicKey: this.keyPair.publicKey.toString("utf8")}, null);
                    //console.log(this.producers);

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
                            producerId: clientId,
                            async: message.async ? message.async : false
                        });
                    //this.cache.save();

                    var readyWorkerId = await this.reserveReadyWorker(message);

                    if(readyWorkerId)
                    {   
                        /*this.workPendingStart[readyWorkerId] = 
                        {   queue: message.queue,
                            workId: message.id,
                            pendingSince: (new Date()).getTime()
                        }*/

                        await this.startWork(readyWorkerId, message.queue);

                    }
                    else
                    {   console.log(`[${new Date()}] No workers ready: queueing message ${message.id}`);
                    }

                break;


                case "workComplete":
                    
                    console.log(`Work for message ${message.id} completed by ${clientId}.`);

                    //this.cache.push(`/queues/${message.queue}/worked/${message.workId}`,
                    //    {completed: (new Date()).getTime(), status: "complete", output: message.output}, false);
                    //this.cache.save();
                    await this.cache.delete(`/queues/${message.queue}/worked/${message.workId}`);
                    //await this.cache.save();
                    await this.db.put({_id: uuidEmit(), type: "workOutput", workId: message.workId, queue: message.queue, workerId: clientId, output: JSON.parse(message.output)});
                    //await this.setWorkerReady(clientId, message, true);

                    if(!message.async)
                    {   console.log(`Sending message workComplete for message ${JSON.stringify(message.id)} to ${message.producerId}`);
                        //this.router.send([message.producerId, JSON.stringify({id: uuidEmit(), output: JSON.parse(message.output)})]);
                        var clientPublicKey = this.encrypt ? this.producers[message.producerId].publicKey : null;
                        await this.sendMessage(message.producerId, {id: uuidEmit(), output: JSON.parse(message.output)}, clientPublicKey);
                    }
                    else
                    {   
                        console.log(`Message ${message.id} from producer ${message.producerId} is async: not sending output to producer.`);

                    }

                break;


                /*case "getDiagnostics":

                    var diagnostics = this.getDiagnostics();
                    var clientPublicKey = this.encrypt ? this.producers[clientId].publicKey : null;
                    await this.sendMessage(clientId, {id: uuidEmit(), diagnostics: diagnostics}, clientPublicKey);

                break;*/


                /*case "getWorkers":

                    var workers = this.getWorkers(message.queue);
                    var clientPublicKey = this.encrypt ? this.producers[clientId].publicKey : null;
                    await this.sendMessage(clientId, {id: uuidEmit(), workers: workers}, clientPublicKey);

                break;*/


                case "getWorkResult":

                    var workResult = await this.getWorkResult(message.workId);

                    console.log(`Sending work result for work item ${JSON.stringify(message.workId)} to ${clientId}`);
                    //this.router.send([clientId, JSON.stringify({id: uuidEmit(), workResult: workResult})]);
                    var clientPublicKey = this.encrypt ? this.producers[clientId].publicKey : null;
                    await this.sendMessage(clientId, {id: uuidEmit(), workResult: workResult}, clientPublicKey);

                break;

            }

        }
        
    }


    async reserveReadyWorker(message)
    {   
        //var _this = this;
        var readyWorkerId = null;


        //var release = await this.mutex.acquire();

        try
        {   
            if(!this.workers[message.queue])
            {   this.workers[message.queue] = {};            
            }

            var workerIds = Object.keys(this.workers[message.queue]);
            //var nextWorkerIndex = this.lastWorkerIndex + 1;
            //var timesWrapped = 0;

            if(workerIds.length == 0)
            {   
                console.log(`No workers online`);
            
                return null;

            }


            /*while(true)
            {   
                if(nextWorkerIndex >= workerIds.length)
                {   nextWorkerIndex = 0;
                    timesWrapped++;
                }

                var nextWorkerId = workerIds[nextWorkerIndex];

                var worker = _this.workers[message.queue][nextWorkerId];

                if(worker.status == "ready")
                {   
                    console.log(`Reserving worker ${nextWorkerId}`);

                    _this.lastWorkerIndex = nextWorkerIndex;
                    readyWorkerId =  nextWorkerId;

                    break;

                }
                else
                {   
                    console.log(`Worker ${nextWorkerId} not ready.  Worker status: ${worker.status}`);
                    nextWorkerIndex++;

                    if((nextWorkerIndex > _this.lastWorkerIndex && timesWrapped > 0) || timesWrapped > 1)
                    {
                        break;

                    }

                }

            }*/

            for(var workerId of workerIds)
            {   
                var worker = this.workers[message.queue][workerId];

                if(worker.status == "ready")
                {   
                    console.log(`Reserving worker ${workerId}`);
                    readyWorkerId =  workerId;

                    break;

                }
                else
                {   
                    console.log(`Worker ${workerId} not ready.  Worker status: ${worker.status}.  Total workers: ${workerIds.length}`);

                }

            }

        }
        finally
        {
            //release();

        }

        
        return readyWorkerId;

    }


    async startWork(workerId, queue)
    {   
        var _this = this;


        //var release = await this.mutex.acquire();

        try
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


            console.log(`Assigning work ${workItem.workId} to ${workerId}`);
            
            
            /* Send work to worker */

            var clientPublicKey = _this.encrypt ? _this.workers[queue][workerId].publicKey : null;
            
            await _this.sendMessage(workerId,
                {   command: "execWork",
                    queue: queue,
                    workId: workItem.workId,
                    data: workItem.data,
                    async: workItem.async ? workItem.async : false,
                    producerId: workItem.producerId
                }, clientPublicKey);


            /* Update worker status */

            _this.workPendingStart[workerId] = 
                {   queue: queue,
                    workId: workItem.workId,
                    pendingSince: (new Date()).getTime()
                }
            
            _this.workers[queue][workerId].status = "working";
            //console.log("Set working: ", new Date());


            /* Update cache */

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
            //_this.cache.save();

        }
        finally
        {
            //release();

        }

    }


    async setWorkerOnline(clientId, message)
    {   
        if(this.debug)
        {   console.log(`Processing online for worker ${clientId}`);            
        }

        if(!this.workers[message.queue] || !this.workers[message.queue][clientId])
        {   await this.setWorkerReady(clientId, message);
        }
        else
        {   this.workers[message.queue][clientId].lastActivity = (new Date()).getTime();            
        }

    }


    async setWorkerReady(clientId, message)
    {   
        //var _this = this;

        if(this.debug)
        {   console.log(`Setting worker status for ${clientId} to ready`);            
        }

        if(!this.workers[message.queue])
        {   this.workers[message.queue] = {};            
        }

        if(!this.workers[message.queue][clientId])
        {   this.workers[message.queue][clientId] = {};
        }


        //const release = await this.mutex.acquire();

        try
        {   
            if(this.workPendingStart[clientId])
            {
                console.log(`Ignoring ready command from worker ${clientId} as this worker has work pending start`);

                //release();
    
                
                return;
    
            }
            
            
            this.workers[message.queue][clientId].status = "ready";
            this.workers[message.queue][clientId].lastActivity = (new Date()).getTime();

        }
        finally
        {
            //release();

        }


        if(this.encrypt)
        {
            this.workers[message.queue][clientId].publicKey = message.publicKey;
            await this.sendMessage(clientId, {command: "setKey", publicKey: this.keyPair.publicKey.toString("utf8")});

        }
    
        await this.startWork(clientId, message.queue);

    }


    async sendMessage(clientId, message, clientPublicKey)
    {   
        //this.router.send([clientId, JSON.stringify({id: uuidEmit(), workResult: workResult})]);

        var modifiedMessage = message;

        
        //const release = await this.mutex.acquire();

        try
        {   
            if(!message.id)
            {   modifiedMessage.id = uuidEmit();            
            }

            if(message.command == "setKey" || !this.encrypt)
            {   
                await this.router.send([clientId, JSON.stringify(modifiedMessage)]);

            }
            else
            {   
                var encryptedMessage = WorkStackCrypto.encryptMessage(JSON.stringify(modifiedMessage), clientPublicKey, this.encryptAlgorithm);
                
                await this.router.send([clientId, JSON.stringify(encryptedMessage)]);

            }

        }
        finally
        {   
            //release();

        }
        

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

                var release = await _this.mutex.acquire();
                    
                try
                {   
                    for(var queue of Object.keys(_this.workers))
                    {   
                        for(var workerId of Object.keys(_this.workers[queue]))
                        {   
                            var worker = _this.workers[queue][workerId];
                            console.log(`Inspecting worker ${workerId}: ${JSON.stringify(worker, null, "\t")}`);
                            
                            if(worker.status == "working")
                            {   
                                continue;

                            }
                            
                            var lastActivity = worker.lastActivity;
                            var now = (new Date()).getTime();
                            
                            if(now - lastActivity >= _this.readyExpiry)
                            {   
                                console.log(`Purging expired worker ${workerId}`);
                                delete _this.workers[queue][workerId];

                            }

                        }

                    }

                }
                finally
                {
                    release();

                }

                
                /* Re-queue orphaned work */
        
                for(var workerId of Object.keys(_this.workPendingStart))
                {
                    var pendingWork = _this.workPendingStart[workerId];
                    console.log(`Pending work: ${JSON.stringify(_this.workPendingStart, null, "\t")}`);
                    var now = (new Date()).getTime();

                    if(now - pendingWork.pendingSince >= _this.pendingWorkExpiry)
                    {   
                        console.log(`Requeuing orphaned work item with ID ${pendingWork.workId} for worker ${workerId} pending since ${(new Date(pendingWork.pendingSince)).toString()}`);
                        
                        release = await _this.mutex.acquire();

                        try
                        {   
                            var workItem = _this.cache.getData(`/queues/${pendingWork.queue}/worked/${pendingWork.workId}`);
                            
                            _this.cache.push(`/queues/${pendingWork.queue}/not-started[]`, 
                                {   received: workItem.received,
                                    workId: pendingWork.workId,
                                    data: workItem.data,
                                    producerId: workItem.producerId
                                });

                            _this.cache.delete(`/queues/${pendingWork.queue}/worked/${pendingWork.workId}`);
                            //_this.cache.save();

                            delete _this.workPendingStart[workerId];
                            delete _this.workers[pendingWork.queue][workerId];
                            
                        }
                        finally
                        {
                            release();

                        }

                    }


                }

                _this.cleanup();

            }, _this.cleanupInterval);

    }


    async getWorkResult(workId)
    {
        var response = await this.db.find({selector: {type: "workOutput", workId: workId}});
        var workResult = {};

        if(response.docs.length > 0)
        {   workResult = response.docs[0];
        }

        
        return workResult;

    }


    getWorkers(queue)
    {   var workers = this.workers[queue];
        //console.log("Workers:" , workers);

        return workers ? workers : {};

    }


    getQueues()
    {
        return this.workers ? this.workers : {};

    }


    getPendingWork()
    {
        return this.workPendingStart ? this.workPendingStart : {};

    }


    getDiagnostics()
    {
        var diagnostics = 
        {   queues: this.getQueues(),
            pendingWork: this.getPendingWork()            
        }


        return diagnostics;

    }

}
        