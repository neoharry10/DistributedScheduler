# Structure 
.Net 10

## Web 
MVC 

## Api
Api

## Domain
Schemas

## Data
Db confi

## Tests
xunit tests



# 

We want some distributed scheduling flow with online apis to showcase this test app 

trading ?

weather ?

orders simulation with banking and ticket service ? probably 

# Jobs 


Job	            What it does	                                                        What it proves
SendEmailJob	Payload {to, subject}. Sleeps 200-800ms (fake SMTP), logs “sent”.	    Throughput, parallelism, and idempotency (the same email must not go out twice).
FlakyJob	    Throws randomly about 50% of the time.	                                Retries, exponential backoff, and the max-attempts limit.
PoisonJob	    Always throws.	                                                        Dead-letter state and manual requeue from the dashboard.
SlowJob     	Runs about 60s in a loop, checking the CancellationToken.	            The kill -9 crash recovery test, heartbeats/lease extension, and graceful shutdown.
CpuJob	        Hashes random bytes for a few seconds.	                                Per-worker concurrency caps (it should never run more than N at once).
WebhookJob	    Does a real HTTP POST to a URL, with a timeout.	                        Real I/O failure handling.


