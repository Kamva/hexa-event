#### hexa event implement events feature for Hexa

#### Install
```
go get github.com/kamva/hexa-event
```

__Known errors :__
- when two time subscribe with same subscription name with subscription type : `pulsar.Exclusive` on __pulsar__ driver, you get this error :  
 ```bash
server error: ConsumerBusy: Exclusive consumer is already connected
```
 
#### Proposal:
- [ ] Remove the `HandlerContext` as first param and get error as return param of `EventHandler`, if you got an error, so return negative signal and log the error, otherwise return positive signal to the event broker.
- [ ] Remove the `err` param as lat para of each handler, if occured error, so just log it and send nack, because we should not get any error in our app, if we get error on an event,
 so we don't need to call to the handler, we need to fix it.

#### Todo:
- [x] Add support of protocol buffer to send events. 
- [x] Add `Extra []interface{}` option to the `SubscriptionOptions` to get more features on each subscription relative to each driver. remove list of options in consumerGenerator(we can generate without a consumer generator or simple consumer generator) __[Accepted]__.
- [x] Implement nats-streaming driver 
- [ ] Implement Mock driver 
- [ ] Write Tests.
- [ ] Implement mock
- [ ] Add badges to readme.
- [ ] CI.
