const zmq = require("zeromq/v5-compat");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;


class Router
{
    workers = {};
    router = null;
    listenPort = 5001;
    listenInterface = "*";
    db = "workerDb";


    constructor(args)
    {   this.route = zmq.socket("router");
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.db = args.db ? args.db : this.db;

    }


    start()
    {   
        this.router.bind(`tcp://${listenInterface}:${listenPort}`, 
            function(err)
            {   
                if(err)
                {   throw(err);            
                }

                
                var db = new JsonDB(new JsonDbConfig(this.db, true, false, "/"));

                console.log(`Listening on ${listenInterface}:${listenPort}`);
                
                
                this.router.on("message", function()
                {   
                    var args = Array.apply(null, arguments);
                    var clientId = args[0].toString("utf8");
                    var message = JSON.parse(args[1].toString("utf8"));
                    console.log(`Received message ${JSON.stringify(message)} from client ${clientId}`);
                    
                    switch(message.command)
                    {   
                        case "ready":
                            
                            if(!this.workers[clientId])
                            {   this.workers[clientId] = {};
                            }                    
                            this.workers[clientId].ready = true;
                            //console.log(`Workers: ${JSON.stringify(workers)}`);

                        break;

                        case "executeWork":
                            
                            db.push(`/${message.queue}/${message.id}`, {start: (new Date()).getTime(), status: "in-progress"});

                            for(var workerId of Object.keys(this.workers))
                            {   var worker = this.workers[workerId];

                                if(worker.ready)
                                {   worker.ready = false;
                                    this.router.send([workerId, "", message.job]);
                                    break;                            
                                }

                            }

                        break;


                        case "workComplete":
                            
                            console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.workOutput)}`);
                            this.workers[clientId].ready = true;

                        break;

                    }

                    //console.log(`Got message from ${args[0].toString("utf8")}`);
                    //console.log(`Sending ${args[1].toString("utf8")}`);
                    //router.send(["worker", "", args[1].toString("utf8")]);
                })

            });        
    }

    

}
        

