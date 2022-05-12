const { uuidEmit } = require('uuid-timestamp')
const zmq = require("zeromq/v5-compat");


module.exports = class Producer
{
    producer = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;


    constructor(args)
    {
        this.producer = zmq.socket("dealer");
        this.producer.identity = `producer-${uuidEmit()}`;
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;

    }


    enqueue(message)
    {   
        var modifiedMessage = JSON.parse(message);

        if(!modifiedMessage.id)
        {   modifiedMessage.id = uuidEmit();            
        }

        this.producer.connect(`tcp://${this.routerAddress}:${this.routerPort}`);
        console.log(`Sending message ${JSON.stringify(modifiedMessage)}`);
        
        this.producer.send([JSON.stringify(modifiedMessage)]);
        this.producer.close();

    }

}
