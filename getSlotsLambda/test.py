import json

test_event = {
  "version": "2.0",
  "routeKey": "$default",
  "rawPath": "/cities/slots?city=Amsterdam",
  "rawQueryString": "city=Amsterdam",
  "cookies": [
    "cookie1",
    "cookie2"
  ],
  "headers": {
    "Header1": "value1",
    "Header2": "value1,value2"
  },
  "queryStringParameters": {
    "city": "Amsterdam"
  }
}

