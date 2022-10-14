package jtl_test

var jtl_header_only = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
`

var jtl_good_01 = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
1662749136019,170,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,162,0,100
1662749136192,44,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136237,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
1662749136281,43,get 1KiB.html,200,OK,Thread Group 1-2,text,true,,,2122,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
1662749136325,44,get 1KiB.html,200,OK,Thread Group 1-2,text,true,,,2122,1,2,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136370,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136414,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136458,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0`

var jtl_with_row_errors = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
-1,170,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,162,0,100
1662749136192,"44",get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136281,43,get 1KiB.html,200,OK,Thread Group 1-2,text,fork,,,2122,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
1662749136325,44,get 1KiB.html,200,OK,Thread Group 1-2,text,true,foo,"ten",2122,1,2,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136370,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,"",1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136370,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136414,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1.2,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136414,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136458,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,-1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,"",0
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,"a"
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0`
