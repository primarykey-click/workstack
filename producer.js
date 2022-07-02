const crypto = require("crypto");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq");
const WorkStackCrypto = require(`${__dirname}/local_modules/WorkStackCrypto`);


module.exports = class Producer
{
    producer = null;
    routerAddress = "127.0.0.1";
    routerPort = 5000;
    authKey = null;
    encrypt = false;
    keyLength = 2048;
    keyPair = null;
    encryptAlgorithm = "aes-256-cbc";


    constructor(args)
    {
        this.producer = new zmq.Dealer({routingId: `producer-${uuidEmit()}`});
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.encrypt = args.encrypt ? args.encrypt : this.encrypt;
        this.encryptAlgorithm = args.encryptAlgorithm ? args.encryptAlgorithm : this.encryptAlgorithm;
        this.keyLength = args.keyLength ? args.keyLength : this.keyLength;

        if(this.encrypt)
        {
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

        }

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

        
        await this.producer.send(JSON.stringify({id: uuidEmit(), command: "setGetKey", publicKey: this.keyPair.publicKey.toString("utf8")}));
        var [ routerPublicKeyMessageRaw ] = await this.producer.receive();
        var routerPublicKey = JSON.parse(routerPublicKeyMessageRaw.toString("utf8")).publicKey;
       
        
        console.log(`Sending message with ID ${JSON.stringify(message.id)}`);

        var modifiedMessage = message;

        if(this.encrypt)
        {   
            modifiedMessage = WorkStackCrypto.encryptMessage(JSON.stringify(modifiedMessage), routerPublicKey, this.encryptAlgorithm);

        }

        await this.producer.send(JSON.stringify(modifiedMessage));


        var output = {};

        if(wait)
        {   var [ outputRaw ] = await this.producer.receive();
            output = JSON.parse(outputRaw.toString("utf8"));

            if(output.encrypted)
            {   output = JSON.parse(WorkStackCrypto.decryptMessage(output, this.keyPair.privateKey, output.algorithm));              
            }

        }


        this.producer.close();


        return output;

    }

}
