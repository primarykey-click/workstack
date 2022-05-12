const zmq = require("zeromq/v5-compat");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;


module.exports = class Router
{
    workers = {};
    router = null;
    listenPort = 5001;
    listenInterface = "*";
    dbFile = "workerDb";
    db = null;


    constructor(args)
    {   this.router = zmq.socket("router");
        
        this.listenPort = args.listenPort ? args.listenPort : this.listenPort;
        this.listenInterface = args.listenInterface ? args.listenInterface : this.listenInterface;
        this.dbFile = args.dbFile ? args.dbFile : this.dbFile;

    }


    start()
    {   
        var _this = this;
        this.db = new JsonDB(new JsonDbConfig(this.dbFile, true, false, "/"));


        this.router.bind(`tcp://${this.listenInterface}:${this.listenPort}`, 
            function(err)
            {   
                if(err)
                {   throw(err);            
                }

                
                console.log(`Listening on ${_this.listenInterface}:${_this.listenPort}`);
                
                
                _this.router.on("message", function()
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
                            _this.workers[clientId].ready = true;
                            //console.log(`Workers: ${JSON.stringify(workers)}`);

                        break;
                        

                        case "execWork":
                            
                            _this.db.push(`/${message.queue}/${message.id}`, {start: (new Date()).getTime(), status: "in-progress"});

                            for(var workerId of Object.keys(_this.workers))
                            {   var worker = _this.workers[workerId];

                                if(worker.ready)
                                {   
                                    worker.ready = false;
                                    _this.router.send([workerId, "", JSON.stringify(
                                        {   command: "execWork",
                                            data: message.data
                                        })]);

                                    break;

                                }

                            }

                        break;


                        case "workComplete":
                            
                            console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.output)}`);
                            _this.workers[clientId].ready = true;

                        break;

                    }

                    //console.log(`Got message from ${args[0].toString("utf8")}`);
                    //console.log(`Sending ${args[1].toString("utf8")}`);
                    //router.send(["worker", "", args[1].toString("utf8")]);
                })

            });        
    }

}
        