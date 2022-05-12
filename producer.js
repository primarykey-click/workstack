const { uuidEmit } = require('uuid-timestamp')
const zmq = require("zeromq/v5-compat");


module.exports = class Producer
{
    producer = null;
    routerAddress = "127.0.0.1";
    routerPort = 5001;
    authKey = null;


    constructor(args)
    {
        this.producer = zmq.socket("dealer");
        this.producer.identity = `producer-${uuidEmit()}`;
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.authKey = args.authKey ? args.authKey : this.authKey;

    }


    enqueue(message)
    {   
        var modifiedMessage = message;

        if(!modifiedMessage.id)
        {   
            modifiedMessage.id = uuidEmit();

        }
            
        if(this.authKey)
        {   modifiedMessage.authKey = this.authKey;
        }


        this.producer.connect(`tcp://${this.routerAddress}:${this.routerPort}`);
        console.log(`Sending message ${JSON.stringify(modifiedMessage)}`);
        
        this.producer.send([JSON.stringify(modifiedMessage)]);
        this.producer.close();

    }

}
