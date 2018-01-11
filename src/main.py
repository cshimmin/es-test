#!/usr/bin/env python

import sys
import time
from elasticsearch.exceptions import NotFoundError
import crayfis_data_pb2 as pb
import uuid

# local packages
import util
import config
import messages

if __name__ == "__main__":
    # try to connect to elasticsearch

    print "Connecting to elasticsearch at '%s'..."%config.get('es.server')
    es = util.connect_elastic(config.get('es.server'), config.get('es.timeout'))
    if es:
        print "Connected to elasticsearch!"

    print es
    print es.cluster
    print es.cluster.get_settings()
    print es.cluster.health()

    # create a new index for XBs
    try:
        es.indices.delete('cf-exposure')
    except NotFoundError:
        pass

    es.indices.create('cf-exposure', body={
        'mappings': {
            'exposure': {
                'properties': {
                    'device_id': { 'type': 'keyword' },
                    'start_time': { 'type': 'date' },
                    'end_time': { 'type': 'date' },
                    'location': { 'type': 'geo_point' },
                    },
                },
            }
        })

    try:
        es.indices.delete('cf-event')
    except NotFoundError:
        pass
    es.indices.create('cf-event', body={
        'mappings': {
            'event': {
                'properties': {
                    'device_id': { 'type': 'keyword' },
                    'timestamp': { 'type': 'date' },
                    'location': { 'type': 'geo_point' },
                    },
                },
            }
        })

    # insert some protobuf data into the db
    max_files = config.get('testdata.maxfiles')
    data_path = config.get('testdata.path')
    do_create_events = config.get('testdata.create.event')
    print "Loading data from", data_path
    tstart = time.time()
    for ifile, msgfile in enumerate(messages.load_messages(data_path)):
        if max_files>0 and ifile >= max_files: break

        msg = pb.CrayonMessage.FromString(msgfile.read())
        chunk = pb.DataChunk.FromString(msg.payload)

        for rc in chunk.run_configs:
            run_id = uuid.UUID(int=rc.id_lo)
            result = es.index(
                index='cf-runconfig',
                doc_type='runconfig',
                id=run_id,
                body={
                    'crayfis_build': rc.crayfis_build,
                    'start_time': rc.start_time,
                    })
            # update any existing exposure blocks referring to this run
            es.update_by_query(
                index='cf-exposure',
                doc_type='exposure',
                body={
                    "script" : "ctx._source.crayfis_build = '%s'"%(rc.crayfis_build),
                    "query": {
                        "term": {
                            "run_id": str(run_id),
                            },
                        },
                    })

        for xb in chunk.exposure_blocks:
            # try to find the runconfig for this XB
            try:
                rc = es.get(index='cf-runconfig', doc_type='runconfig', id=uuid.UUID(int=xb.run_id))
            except NotFoundError:
                rc = None
            
            xb_id = util.make_uuid("%s%d"%(msg.device_id, xb.start_time))
            result = es.index(
                index='cf-exposure',
                doc_type='exposure',
                id=xb_id,
                body=messages.xb_to_doc(msg, xb, rc)
            )
            if ifile < 20:
                print "Inserted XB:"
                print result
                print 
            elif ifile == 20:
                print "Squelching further output."
                sys.stdout.flush()

            if do_create_events:
                for evt in xb.events:
                    event_id = util.make_uuid("%s%d"%(xb_id, evt.timestamp))
                    result = es.index(
                        index='cf-event',
                        doc_type='event',
                        id=event_id,
                        body=messages.event_to_doc(msg, xb, evt)
                    )

        # done processing.
        if (ifile+1)%500==0:
            elapsed = time.time()-tstart
            print 'Processed %d files in %ds (%0.2fHz)' % (ifile+1, elapsed, ifile/elapsed)
            sys.stdout.flush()



    print "refreshing exposure index..."
    es.indices.refresh(index='cf-exposure')
    result = es.search(index='cf-exposure')
    print "Query result:"
    print result
    print len(result['hits'])
    print 


    # now a more complicated search aggregating by device ID
    result = es.search(index='cf-exposure', body={
        'size': 0,
        'aggs': {
            'devices': {
                'terms': {
                    'field': 'device_id',
                    },
                'aggs': {
                    'average_temp': {
                        'avg': {
                            'field': 'battery_temp',
                            }
                        },
                    'sum_temp_weighted': {
                        'sum': {
                            'field': 'weighted_temp',
                            }
                        },
                    'total_exposure': {
                        'sum': {
                            'field': 'duration',
                            }
                        },
                    'total_L1_passes': {
                        'sum': {
                            'field': 'n_events',
                            }
                        },
                    'total_L2_skips': {
                        'sum': {
                            'field': 'L2_skip',
                            }
                        },
                    'total_L1_processed': {
                        'sum': {
                            'field': 'L1_processed',
                            }
                        },
                    },
                },
            }
        })

    print "Agg query result:"
    print result
    print

    for d in result['aggregations']['devices']['buckets']:
        print d

    for d in result['aggregations']['devices']['buckets']:
        total_exposure = d['total_exposure']['value']
        average_temp = d['average_temp']['value']
        sum_temp_weighted = d['sum_temp_weighted']['value']
        l2_skips = d['total_L2_skips']['value']
        l1_passes = d['total_L1_passes']['value']
        sum_L1_processed = d['total_L1_processed']['value']
        device_id = d['key']

        print "Device %s:" % device_id
        print '  n blocks:       ', d['doc_count']
        print '  total exposure: ', total_exposure
        print '  average temp:   ', average_temp
        print '  weighted avg:   ', sum_temp_weighted/total_exposure
        print '  L2_skip/L1_pass:  %d/%d=%0.5f%%'%(l2_skips, l1_passes, 100.*l2_skips/l1_passes if (l1_passes>0) else 0)
        print '  avg framerate:  ', sum_L1_processed/total_exposure*1e3
        print
