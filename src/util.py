import uuid
# Define a fixed namespace UUID for synthesizing deterministic
# uuid's for e.g. exposure blocks.
NAMESPACE_UUID = uuid.UUID(int=1234)

# Return a deterministic UUID from any (unique) string
def make_uuid(to_hash):
    return uuid.uuid3(NAMESPACE_UUID, to_hash.encode('utf-8'))

# Connect to an elasticsearch server, blocking until the connection
# is successful or until the specified timeout has elapsed.
def connect_elastic(server, timeout, retry_period=1, verbose=True):
    from elasticsearch import Elasticsearch
    import time
    import sys
    es = Elasticsearch([server])
    tstart = time.time()
    while not es.ping():
        elapsed = time.time()-tstart
        if elapsed > timeout:
            if verbose:
                print "Connection to elasticsearch at '%s' failed after %ds"%(server, elapsed)
            return None
        if verbose:
            print "No connection to elasticsearch at '%s', will retry in %ds"%(server, retry_period)
            sys.stdout.flush()
        time.sleep(retry_period)
    return es
