const zmq = require("zeromq/v5-compat");
const { JsonDB } = require("node-json-db");
const JsonDbConfig = require("node-json-db/dist/lib/JsonDBConfig").Config;


var workers = {};
var router = zmq.socket("router");
var listenPort = 5001;
var listenInterface = "*";


router.bind(`tcp://${listenInterface}:${listenPort}`, err =>
    {   
        if(err)
        {   throw(err);            
        }

        
        var db = new JsonDB(new JsonDbConfig("workerDb", true, false, "/"));

        console.log(`Listening on ${listenInterface}:${listenPort}`);
        
        
        router.on("message", function()
        {   
            var args = Array.apply(null, arguments);
            var clientId = args[0].toString("utf8");
            var message = JSON.parse(args[1].toString("utf8"));
            console.log(`Received message ${JSON.stringify(message)} from client ${clientId}`);
            
            switch(message.command)
            {   
                case "ready":
                    
                    if(!workers[clientId])
                    {   workers[clientId] = {};
                    }                    
                    workers[clientId].ready = true;
                    //console.log(`Workers: ${JSON.stringify(workers)}`);

                break;

                case "executeWork":
                    
                    db.push(`/${message.queue}/${message.id}`, {start: (new Date()).getTime(), status: "in-progress"});

                    for(var workerId of Object.keys(workers))
                    {   var worker = workers[workerId];

                        if(worker.ready)
                        {   worker.ready = false;
                            router.send([workerId, "", message.job]);
                            break;                            
                        }

                    }

                break;


                case "workComplete":
                    
                    console.log(`Work completed by ${clientId}. Result: ${JSON.stringify(message.workOutput)}`);
                    workers[clientId].ready = true;

                break;

            }

            //console.log(`Got message from ${args[0].toString("utf8")}`);
            //console.log(`Sending ${args[1].toString("utf8")}`);
            //router.send(["worker", "", args[1].toString("utf8")]);
        })

    });
        

