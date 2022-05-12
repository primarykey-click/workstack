const zmq = require("zeromq/v5-compat");
const uuid = require("uuid").v4;


module.exports = class Producer
{
    producer = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;


    constructor(args)
    {
        this.producer = zmq.socket("dealer");
        this.producer.identity = `producer-${uuid()}`;
        this.routerAddress = args.routerAddress ? args.routerAddress : "127.0.0.1";
        this.routerPort = args.routerPort ? args.routerPort : 5001;

    }


    enqueue(message)
    {   
        var modifiedMessage = message;

        if(!modifiedMessage.id)
        {   modifiedMessage = uuid();            
        }

        this.producer.connect(`tcp://${this.routerAddress}:${this.routerPort}`);
        console.log(`Sending message ${modifiedMessage}`);
        
        this.producer.send([modifiedMessage]);
        this.producer.close();

    }

}


/*(   
    async()=>
    {   
        var producer = zmq.socket("dealer");

        producer.identity = `producer`;
        producer.connect("tcp://127.0.0.1:5001")
        console.log(`Sending message ${process.argv[2]}`);
        console.log(JSON.parse(process.argv[2]));
        producer.send([process.argv[2]]);
        producer.close();

    }

)()*/