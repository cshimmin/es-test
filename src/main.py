#!/usr/bin/env python

import sys
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import crayfis_data_pb2 as pb
from glob import glob
import uuid

NAMESPACE_UUID = uuid.UUID(int=1234)

def make_uuid(to_hash):
    return uuid.uuid3(NAMESPACE_UUID, to_hash.encode('utf-8'))

def xb_to_doc(msg, xb):
    duration = xb.end_time - xb.start_time
    return {
            'start_time': xb.start_time,
            'end_time': xb.end_time,
            'duration': duration,
            'device_id': msg.device_id,
            'battery_temp': xb.battery_temp,
            'weighted_temp': xb.battery_temp*duration,
            'L1_thresh': xb.L1_thresh,
            'L2_skip': xb.L2_skip,
            'bg_avg': xb.bg_avg,
            'n_events': len(xb.events),
            'n_pixels': sum([len(evt.pixels) for evt in xb.events]),
            'event_rate': len(xb.events)/duration,
            'L1_processed': xb.L1_processed,
    }

if __name__ == "__main__":
    es = Elasticsearch(['elastic'])

    # create an index for XBs
    INDEX_NAME = 'test-index'
    try:
        es.indices.delete(INDEX_NAME)
    except NotFoundError:
        pass

    es.indices.create(INDEX_NAME, body={
        'mappings': {
            'exposure': {
                'properties': {
                    'device_id': {
                        'type': 'text',
                        'fielddata': True,
                        }
                    }
                }
            }
        })

    # insert some protobuf data into the db
    MAX_FILES = 100
    for ifile, fname in enumerate(glob("test-data/*.msg")):
        if ifile >= MAX_FILES: break

        msg = pb.CrayonMessage.FromString(open(fname).read())
        chunk = pb.DataChunk.FromString(msg.payload)

        for xb in chunk.exposure_blocks:
            xb_id = make_uuid("%s%d"%(msg.device_id, xb.start_time))
            result = es.index(
                index=INDEX_NAME,
                doc_type='exposure',
                id=xb_id,
                body=xb_to_doc(msg, xb)
            )
            if ifile < 20:
                print "Inserted XB:"
                print result
                print 
            elif ifile == 100:
                print "Squelching further output."
                sys.stdout.flush()

    print "refreshing index..."
    es.indices.refresh(index=INDEX_NAME)
    # now try a query to just grab all of the XBs

    #result = es.get(index=INDEX_NAME, doc_type='exposure', id='34ad2765-a098-35c8-9263-db8779c8e996')
    #print result

    #result = es.search(index=INDEX_NAME, body={
    #    "query": {"match_all": {}}
    #    })
    result = es.search(index=INDEX_NAME)
    print "Query result:"
    print result
    print len(result['hits'])
    print 


    # now a more complicated search aggregating by device ID
    result = es.search(index=INDEX_NAME, body={
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
        print '  L2_skip/L1_pass:  %d/%d=%0.5f%%'%(l2_skips, l1_passes, 100.*l2_skips/l1_passes)
        print '  avg framerate:  ', sum_L1_processed/total_exposure*1e3
        print
