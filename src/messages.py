# Generator to load messages from tar.gz files located in a directory with
# the usual YYYY/MM/DD/HOST/HH.tar.gz structure.
# The indiviual .msg files are extracted from the tarfile and returned as
# file objects.
def load_messages(data_path, verbose=False):
    from glob import glob
    import tarfile
    import os.path
    for tfile_name in sorted(glob(os.path.join(data_path,"*/*/*/*/*.tar.gz"))):
        if verbose:
            print "Opening tarfile:", tfile_name
        tfile = tarfile.open(tfile_name)
        for f_info in tfile.getmembers():
            if not f_info.isfile(): continue
            if not f_info.name.endswith(".msg"): continue

            if verbose:
                print "  Extracting message file:", f_info.name
            yield tfile.extractfile(f_info)

# convert an exposure block (with a cryon message as context) to a
# doctument representation for indexing into elasticsearch.
def xb_to_doc(msg, xb, rc):
    duration = xb.end_time - xb.start_time
    duration_nano = xb.end_time_nano - xb.start_time_nano
    document = {
        'start_time': xb.start_time,
        'end_time': xb.end_time,
        'start_time_ntp': xb.start_time_ntp,
        'duration': duration,
        'duration_nano': duration_nano,
        'device_id': msg.device_id,
        'battery_temp': xb.battery_temp,
        'weighted_temp': xb.battery_temp*duration,
        'L1_thresh': xb.L1_thresh,
        'L2_skip': xb.L2_skip,
        'bg_avg': xb.bg_avg,
        'n_events': len(xb.events),
        'n_pixels': sum([len(evt.pixels) for evt in xb.events]),
        'event_rate': len(xb.events)/duration if duration>0 else 0,
        'L1_processed': xb.L1_processed,
        'location': [ xb.gps_lon, xb.gps_lat ],
    }
    if rc:
        document['crayfis_build'] = rc['_source']['crayfis_build']
    else:
        document['crayfis_build'] = 'unknown'

    return document

def event_to_doc(msg, xb, evt):
    document = {
            'device_id': msg.device_id,
            'timestamp': evt.timestamp,
            'location': [ xb.gps_lon, xb.gps_lat ],
            'n_pix': len(evt.pixels),
            'avg': evt.avg,
            'std': evt.std,
            'battery_temp': evt.battery_temp,
            'pix.val': [],
            'pix.val_adj': [],
            'pix.x': [],
            'pix.y': [],
    }
    for p in evt.pixels:
        document['pix.val'].append(p.val)
        document['pix.val_adj'].append(p.adjusted_val)
        document['pix.x'].append(p.x)
        document['pix.y'].append(p.y)

    return document
