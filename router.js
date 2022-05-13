const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;
const { Mutex } = require("async-mutex");
//const zmq = require("zeromq/v5-compat");
const zmq = require("zeromq");


module.exports = class Router
{
    workers = {};
    router = null;
    replyRouter = null;
    listenPort = 5000;
    replyListenPort = 5002;
    listenInterface = "*";
    dbFile = "db.json";
    db = null;
    authMethod = "none";
    authKey = "auth.key";


    constructor(args)
    {   //this.router = zmq.socket("router");
        this.router = new zmq.Router();
        //this.replyRouter = zmq.socket("rep");
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.replyListenPort = args.replyListenPort ? args.replyListenPort : this.replyListenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.dbFile = args.dbFile ? args.dbFile : this.dbFile;
        this.authMethod = args.authMethod ? args.authMethod : this.authMethod;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.mutex = new Mutex();

    }


    async start()
    {   
        this.db = new JsonDB(new JsonDbConfig(this.dbFile, true, true, "/"));

        try
        {   this.workers = this.db.getData("/workers");
        }
        catch(err)
        {   
            console.log("No workers registered");

        }


        await this.router.bind(`tcp://${this.listenInterface}:${this.listenPort}`);

        console.log(`Listening on ${this.listenInterface}:${this.listenPort}`);


        for(var workerId of Object.keys(this.workers))
        {   
            this.workers[workerId].status = "pending";
            this.db.push(`/workers/${workerId}`, this.workers[workerId]);
            
            console.log(`Sending confirmReady command to ${workerId}`);

            this.router.send([workerId, "", JSON.stringify(
                {   command: "confirmReady"
                })]);

        }


        for await (var [id, msg] of this.router)
        {   var clientId = id.toString("utf8");
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

            console.log(`Received message ${JSON.stringify(message)} from client ${clientId}`);

            
            switch(message.command)
            {   
                case "ready":
                    
                    if(!this.workers[clientId])
                    {   this.workers[clientId] = {};
                    }

                    this.workers[clientId].status = "ready";
                    this.workers[clientId].lastActivity = (new Date()).getTime();

                    this.db.push(`/workers/${clientId}`, this.workers[clientId]);

                    this.startWork(clientId, message.queue);
                    

                break;


                case "execWork":
                    
                    this.db.push(`/queues/${message.queue}/not-started[]`, 
                        {   received: (new Date()).getTime(),
                            workId: message.id,
                            data: message.data,
                            producerId: clientId
                        });

                    for(var workerId of Object.keys(this.workers))
                    {   
                        var worker = this.workers[workerId];

                        if(worker.status == "ready")
                        {   
                            this.startWork(workerId, message.queue);

                            break;

                        }

                    }

                break;


                case "workComplete":
                    
                    console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.output)}`);
                    this.workers[clientId].status = "ready";

                    this.db.push(`/queues/${message.queue}/worked/${message.workId}`,
                        {completed: (new Date()).getTime(), status: "complete", output: message.output}, false);

                    console.log(`Sending message ${JSON.stringify(message)} to ${message.producerId}`);
                    this.router.send([message.producerId, {output: message.output}]);

                break;

            }

        }
        
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
                        console.log(`No work items in queue ${queue}`);

                    }
                    else
                    {   console.log(err);                       
                    }
                    //console.log(err.message);
                    //console.log(`No work items`);
                
                    return;

                }


                if(!workItem)
                {
                    console.log(`No work items found`);

                    return;

                }


                console.log("Work item: ", JSON.stringify(workItem));
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
                        producerId: workItem.producerId,
                        data: workItem.data
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
        