const crypto = require("crypto");
const express = require("express");
const fs = require("fs");
const PouchDB = require("pouchdb-node");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq");
//const { fstat } = require("fs");
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
    readyExpiry = 60000;
    workingExpiry = 60000;
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
    privateKeyPath = "./data/crypto/private.key";
    publicKeyPath = "./data/crypto/public.key";


    constructor(args)
    {   
        this.router = new zmq.Router({sendHighWaterMark: 0});
        //this.events = this.router.events;
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.managementPort = args.managementPort ? args.managementPort : this.managementPort;
        this.replyListenPort = args.replyListenPort ? args.replyListenPort : this.replyListenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.readyExpiry = args.readyExpiry ? args.readyExpiry : this.readyExpiry;
        this.workingExpiry = args.workingExpiry ? args.workingExpiry : this.workingExpiry;
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
        this.privateKeyPath = args.privateKeyPath ? args.privateKeyPath : this.privateKeyPath;
        this.publicKeyPath = args.publicKeyPath ? args.publicKeyPath : this.publicKeyPath;
        this.mutex = new Mutex();

        if(this.encrypt)
        {
            this.initKeyPair();

        }

    }


    initKeyPair()
    {
        this.log("Initializing key pair");


        if(!fs.existsSync(`./data`))
        {   fs.mkdirSync(`./data`);
        }

        if(!fs.existsSync(`./data/crypto`))
        {   fs.mkdirSync(`./data/crypto`);
        }

        if(!fs.existsSync(this.privateKeyPath))
        {
            this.log(`Creating keypair.`);

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

            
            fs.writeFileSync(this.privateKeyPath, this.keyPair.privateKey);
            fs.writeFileSync(this.publicKeyPath, this.keyPair.publicKey);

            this.log(`Created key pair.`);
            this.log(`Private key: ${this.privateKeyPath}`);
            this.log(`Public key: ${this.publicKeyPath}`);

        }
        else
        {
            this.keyPair = {};
            this.keyPair.privateKey = fs.readFileSync(this.privateKeyPath, {encoding: "utf-8"});
            this.keyPair.publicKey = fs.readFileSync(this.publicKeyPath, {encoding: "utf-8"});

        }

    }


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


        app.get("/findWorkers", async function(req, res, next)
            {   
                try
                {   
                    var workers = _this.findWorkers(req.query.queueExpr);
                    res.json(workers);

                }
                catch(err)
                {
                    next(err);

                }
                                
            });


        app.get("/queueData", async function(req, res, next)
            {   
                try
                {   
                    var queueData = _this.getQueueData();
                    var queueDataFiltered = {};

                    for(var itemKey of Object.keys(queueData))
                    {   var item = queueData[itemKey];
                        queueDataFiltered[itemKey] = item["not-started"].length;
                    }


                    res.json(queueDataFiltered);

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

            if(message.command != "online")
            {   console.log(`Received command ${message.command} within message with ID ${JSON.stringify(message.id)} from client ${clientId}`);
            }

            
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


                case "setGetKey":

                    if(!this.producers[clientId])
                    {   this.producers[clientId] = {};                        
                    }

                    this.producers[clientId].publicKey = message.publicKey;
                    await this.sendMessage(clientId, {command: "setKey", publicKey: this.keyPair.publicKey.toString("utf8")}, null);

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
                        await this.startWork(readyWorkerId, message.queue);

                    }
                    else
                    {   this.log(`[${new Date()}] No workers ready: queueing message ${message.id}`);
                    }

                break;


                case "workComplete":
                    
                    this.log(`Work for message ${message.id} completed by ${clientId}.`);

                    await this.cache.delete(`/queues/${message.queue}/worked/${message.workId}`);
                    //await this.cache.save();
                    this.db.put({_id: uuidEmit(), type: "workOutput", workId: message.workId, queue: message.queue, workerId: clientId, output: JSON.parse(message.output)});

                    if(!message.async)
                    {   this.log(`Sending message workComplete for message ${JSON.stringify(message.id)} to ${message.producerId}`);
                        var clientPublicKey = this.encrypt ? this.producers[message.producerId].publicKey : null;
                        await this.sendMessage(message.producerId, {id: uuidEmit(), output: JSON.parse(message.output)}, clientPublicKey);
                    }
                    else
                    {   
                        this.log(`Message ${message.id} from producer ${message.producerId} is async: not sending output to producer.`);

                    }

                break;


                case "getWorkResult":

                    var workResult = await this.getWorkResult(message.workId);

                    this.log(`Sending work result for work item ${JSON.stringify(message.workId)} to ${clientId}`);
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

            if(workerIds.length == 0)
            {   
                this.log(`No workers online`);
            
                return null;

            }


            for(var workerId of workerIds)
            {   
                var worker = this.workers[message.queue][workerId];

                if(worker.status == "ready")
                {   
                    this.log(`Reserving worker ${workerId}`);
                    readyWorkerId =  workerId;

                    break;

                }
                else
                {   
                    this.log(`Worker ${workerId} not ready.  Worker status: ${worker.status}.  Total workers: ${workerIds.length}`);

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
        //var _this = this;


        //var release = await this.mutex.acquire();

        try
        {
            var workItem = null;

            
            try
            {   
                workItem = this.cache.getData(`/queues/${queue}/not-started[0]`);

            }
            catch(err)
            {   
                if(err.message && err.message.match(/(Can't find dataPath)|(Can't find index)/g))
                {
                    if(this.debug)
                    {   this.log(`No work items in queue ${queue}`);
                    }

                }
                else
                {   console.log(err);                       
                }

            
                return;

            }


            if(!workItem)
            {
                this.log(`No work items found`);

                return;

            }


            this.log(`Assigning work ${workItem.workId} to ${workerId}`);
            
            
            /* Send work to worker */

            if(this.encrypt)
            {
                await this.sendMessage(workerId, {command: "setKey", publicKey: this.keyPair.publicKey.toString("utf8")});

            }


            var clientPublicKey = this.encrypt ? this.workers[queue][workerId].publicKey : null;
            
            await this.sendMessage(workerId,
                {   command: "execWork",
                    queue: queue,
                    workId: workItem.workId,
                    data: workItem.data,
                    async: workItem.async ? workItem.async : false,
                    producerId: workItem.producerId
                }, clientPublicKey);


            /* Update worker status */

            this.workPendingStart[workerId] = 
                {   queue: queue,
                    workId: workItem.workId,
                    pendingSince: (new Date()).getTime()
                }
            
            this.workers[queue][workerId].status = "working";
            //console.log("Set working: ", new Date());


            /* Update cache */

            var workItemIndex = this.cache.getIndex(`/queues/${queue}/not-started`, workItem.workId, "workId");
            
            this.cache.delete(`/queues/${queue}/not-started[${workItemIndex}]`);
            this.cache.push(`/queues/${queue}/worked/${workItem.workId}`, 
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
        {   this.log(`Processing online for worker ${clientId}`);            
        }

        if(!this.workers[message.queue] || !this.workers[message.queue][clientId])
        {   await this.setWorkerReady(clientId, message);
        }
        else
        {   
            if(this.workers[message.queue][clientId].status == "ready")
            {   await this.setWorkerReady(clientId, message);
            }
            else
            {   this.workers[message.queue][clientId].lastActivity = (new Date()).getTime();            
            }

        }

    }


    async setWorkerReady(clientId, message)
    {   
        //var _this = this;

        if(this.debug)
        {   this.log(`Setting worker status for ${clientId} to ready`);            
        }

        if(!this.workers[message.queue])
        {   this.workers[message.queue] = {};            
        }

        if(!this.workers[message.queue][clientId])
        {   this.workers[message.queue][clientId] = {};
        }

        
        if(message.meta)
        {   this.workers[message.queue][clientId].meta = message.meta;
        }


        //const release = await this.mutex.acquire();

        try
        {   
            if(this.workPendingStart[clientId])
            {
                this.log(`Ignoring ready command from worker ${clientId} as this worker has work pending start`);

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
            //await this.sendMessage(clientId, {command: "setKey", publicKey: this.keyPair.publicKey.toString("utf8")});

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
        {   this.log(`Cleaning up`);
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
                            
                            if(_this.debug)
                            {   _this.log(`Inspecting worker ${workerId}: ${JSON.stringify(worker, null, "\t")}`);
                            }

                            var lastActivity = worker.lastActivity;
                            var now = (new Date()).getTime();
                            
                            if(worker.status == "working")
                            {   
                                if(now - lastActivity >= _this.workingExpiry)
                                {   
                                    _this.log(`Purging expired worker ${workerId} regardless of "working" state (appears to have aborted)`);
                                    delete _this.workers[queue][workerId];

                                }

                                //continue;

                            }
                            else if(now - lastActivity >= _this.readyExpiry)
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
    {   
        var workers = this.workers[queue];

        return workers ? workers : {};

    }


    findWorkers(queueExpr)
    {   
        var workers = {};
        
        for(var queue of Object.keys(this.workers))
        {
            if(queue.match(new RegExp(queueExpr, "ig")))
            {
                workers = {...workers, ...this.workers[queue]}

            }

        }


        return workers;

    }


    getQueues()
    {
        return this.workers ? this.workers : {};

    }


    getQueueData()
    {   
        var queueData = [];

        try
        {
            queueData = this.cache.getData("/queues");

        }
        catch(err)
        {
            if(err.message && err.message.match(/(Can't find dataPath)|(Can't find index)/g))
            {
                if(this.debug)
                {   console.log(`No work items in queue ${queue}`);
                }

            }
            else
            {   console.log(err);                       
            }

        }
        
        
        return queueData;



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


    log(message)
    {
        console.log(`[${(new Date()).toLocaleString("en-ca")}] ${message}`);

    }

}
        