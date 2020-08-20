 hestan (hexa stan) in implementation of Nats-streaming broker for hexa SDK using stan client library of NATS.
 
 -------
 
 __Notes:__
 - If you need to set unsubscribe options for a subscription, you can just add a function to the subscription options which gets subscription and decide whether should close or unsubscribe to it if user invoke `close` method of the receiver, but for now we don't need to it and just close all subscriptions if user invoke `close` method of the `Receiver`.
 
 - We do not support multichannel and channelPattern in subscription options.