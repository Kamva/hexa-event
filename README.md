#### hexa event implement events feature for Hexa

#### Install
```
go get github.com/Kamva/hexa-event
```

__Known errors :__
- when two time subscribe with same subscription name with subscription type : `pulsar.Exclusive` on __pulsar__ driver, you get this error :  
 ```bash
server error: ConsumerBusy: Exclusive consumer is already connected
```
 
#### Proposal:
- [x] Add `Extra []interface{}` option to the the `SubscriptionOptions` to get more features on each subscription relative to each driver. remove list of options in consumerGenerator(we can generate without a consumer generator or simple consumer generator) __[Accepted]__.

#### Todo:
- [x] Add support of protocol buffer to send events. 
- [ ] Mock driver 
- [ ] Write Tests.
- [ ] Implement mock
- [ ] Add badges to readme.
- [ ] CI.