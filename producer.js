const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq");


module.exports = class Producer
{
    producer = null;
    routerAddress = "127.0.0.1";
    routerPort = 5000;
    authKey = null;


    constructor(args)
    {
        this.producer = new zmq.Dealer({routingId: `producer-${uuidEmit()}`});
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.authKey = args.authKey ? args.authKey : this.authKey;

    }


    async enqueue(message, wait)
    {   
        if(!message.id)
        {   message.id = uuidEmit();
        }
            
        if(this.authKey)
        {   message.authKey = this.authKey;
        }


        this.producer.connect(`tcp://${this.routerAddress}:${this.routerPort}`);
        
        console.log(`Sending message with ID ${JSON.stringify(message.id)}`);
        await this.producer.send(JSON.stringify(message));


        var output = {};

        if(wait)
        {   var [ outputRaw ] = await this.producer.receive();
            output = JSON.parse(outputRaw.toString("utf8"));
        }


        this.producer.close();


        return output;

    }

}
