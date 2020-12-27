Outbox pattern for kafka.

The outbox document example:
```js
var doc={
topic:"order.created",
key:"my_key",
value:"my_value",
headers:[{key:"header_key",value:"header_value"}]
}
```