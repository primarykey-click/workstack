const crypto = require("crypto");
const { Mutex } = require("async-mutex");
const { uuidEmit } = require("uuid-timestamp")
const zmq = require("zeromq/v5-compat");
const WorkStackCrypto = require(`${__dirname}/local_modules/WorkStackCrypto`);


module.exports = class Worker
{   
    worker = null;
    routerAddress = "127.0.0.1";
    routerPort = 5000;
    pingInterval = 30000;
    debug = false;
    queue = null;
    mutex = null;
    status = null;
    authKey = null;
    encrypt = false;
    keyLength = 2048;
    keyPair = null;
    encryptAlgorithm = "aes-256-cbc";
    routerPublicKey = null;


    constructor(args)
    {
        this.worker = zmq.socket("dealer");
        this.routerAddress = args.routerAddress ? args.routerAddress : this.routerAddress;
        this.routerPort = args.routerPort ? args.routerPort : this.routerPort;
        this.pingInterval = args.pingInterval ? args.pingInterval : this.pingInterval;
        this.debug = args.debug ? args.debug : false;
        this.authKey = args.authKey ? args.authKey : this.authKey;
        this.encrypt = args.encrypt ? args.encrypt : this.encrypt;
        this.keyLength = args.keyLength ? args.keyLength : this.keyLength;
        this.encryptAlgorithm = args.encryptAlgorithm ? args.encryptAlgorithm : this.encryptAlgorithm;
        this.queue = args.queue;
        this.worker.identity = `worker-${args.workerId ? args.workerId : uuidEmit()}`;
        this.worker.work = args.work;
        this.mutex = new Mutex();

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

  
    async start()
    {   
        var _this = this;

        console.log(`Worker ${this.worker.identity} ready`);

        this.worker.connect(`tcp://${this.routerAddress}:${this.routerPort}`)

        await this.sendReady();

        this.pingRouter();

        
        //this.worker.on("message", async function(id, msg)
        this.worker.on("message", async function(msg)
            {   
                var rawMessage = JSON.parse(msg.toString("utf8"));
                var message = rawMessage.encrypted ? JSON.parse(WorkStackCrypto.decryptMessage(rawMessage, _this.keyPair.privateKey)) : rawMessage;

                if(message.command != "setKey") 
                {   console.log(`Received message with ID ${message.id} and command ${message.command}`);
                }


                switch(message.command)
                {
                    case "execWork":
                        
                        //var release = await _this.mutex.acquire();

                        try
                        {   
                            _this.status = "working";
                            await _this.sendMessage({command: "working"});
                            
                            var output = await _this.worker.work({workId: message.workId, data: message.data});
                            await _this.sendMessage(
                                {   command: "workComplete",
                                    queue: message.queue,
                                    workId: message.workId,
                                    producerId: message.producerId,
                                    output: JSON.stringify(output)
                                });
                            
                            //await _this.sendReady();

                        }
                        finally
                        {
                            //release();

                        }


                    break;


                    case "setKey":
                        
                        //var release = await _this.mutex.acquire();

                        try
                        {   
                            _this.routerPublicKey = message.publicKey;

                        }
                        finally
                        {
                            //release();

                        }

                    break;

                }

            });


        process.on("SIGINT", async function(){await _this.exitClean();});
        process.on("SIGTERM", async function(){await _this.exitClean();});

    }


    async exitClean()
    {   
        await this.sendMessage({command: "offline", queue: this.queue});
        this.worker.close();
        process.exit();

    }


    pingRouter()
    {   
        var _this = this;

        setTimeout(async function()
            {   
                var release = await _this.mutex.acquire();

                try
                {   
                    if(_this.status == "ready")
                    {   
                        await _this.sendReady();

                    }

                }
                finally
                {
                    release();

                }
                
                _this.pingRouter();

            }, this.pingInterval);

    }


    async sendMessage(message)
    {   
        var modifiedMessage = message;

        if(this.authKey)
        {   modifiedMessage.authKey = this.authKey;
        }

        if(!message.id)
        {   modifiedMessage.id = uuidEmit();            
        }

        if(this.debug)
        {   console.log(`Sending message ${JSON.stringify(message)}`);
        }
        else
        {   
            if(message.command != "ready" || (message.command == "ready" && this.debug))
            {   console.log(`Sending message with ID ${message.id} and command "${message.command}"`);            
            }

        }

        if(message.command == "ready" || !this.encrypt)
        {
            await this.worker.send([JSON.stringify(modifiedMessage)]);

        }
        else
        {   
            var encryptedMessage = WorkStackCrypto.encryptMessage(JSON.stringify(modifiedMessage), this.routerPublicKey, this.encryptAlgorithm);
            
            await this.worker.send([JSON.stringify(encryptedMessage)]);

        }
        

    }


    async sendReady()
    {   
        if(this.debug)
        {   console.log(`[${(new Date()).toString()}] Sending ready`);            
        }

        this.status = "ready";

        if(this.encrypt)
        {   
            await this.sendMessage(
                {   command: "ready", 
                    queue: this.queue, 
                    publicKey: this.keyPair.publicKey.toString("utf8")
                });

        }
        else
        {   
            await this.sendMessage(
                {   command: "ready", 
                    queue: this.queue
                });

        }

    }

}