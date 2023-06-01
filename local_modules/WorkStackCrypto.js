
const crypto = require("crypto");


exports.encryptMessage = 
    function(message, publicKey, algorithm)
    {   var password = crypto.randomBytes(32);
        var iv = crypto.randomBytes(16);
        var encryptedPassword = crypto.publicEncrypt(publicKey, Buffer.from(password)).toString("base64");
        var encryptedIv = crypto.publicEncrypt(publicKey, Buffer.from(iv)).toString("base64");
        var cipher = crypto.createCipheriv(algorithm, Buffer.from(password, "hex"), iv);
        var encryptedMessageContent = cipher.update(message);
        encryptedMessageContent = Buffer.concat([encryptedMessageContent, cipher.final()]).toString("base64");
        var encryptedMessage = 
            {   encrypted: true, 
                encryptedPassword: encryptedPassword, 
                encryptedIv: encryptedIv,
                algorithm: algorithm,
                encryptedContent: encryptedMessageContent
            };

        
        
        return encryptedMessage;

    }


exports.decryptMessage = 
    function(message, privateKey)
    {   
        var password = crypto.privateDecrypt(privateKey, Buffer.from(message.encryptedPassword, "base64"));
        var iv = crypto.privateDecrypt(privateKey, Buffer.from(message.encryptedIv, "base64"));
        var decipher = crypto.createDecipheriv(message.algorithm, Buffer.from(password, "hex"), iv);
        var messageContent = decipher.update(Buffer.from(message.encryptedContent, "base64"));
        messageContent = Buffer.concat([messageContent, decipher.final()]).toString("utf8");
        
        
        return messageContent;

    }
