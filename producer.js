const zmq = require("zeromq/v5-compat");
const uuid = require("uuid").v4;


(   
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

)()