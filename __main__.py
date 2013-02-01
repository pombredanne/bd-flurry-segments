from bitdeli.chain import Profiles
from bitdeli.widgets import Title, Description
from collections import Counter
import math

COUNT_LIMIT = 10
TOPN = 10
NUM_BINS = 4
LABELS = ['1'] + ['%d-%d' % (2**i, 2**(i+1)-1) for i in range(1, NUM_BINS)]
text = {} 

def num_sessions(profiles):       
    bins = [0] * NUM_BINS
    for profile in profiles:
        n = sum(1 for s in profile['sessions'])
        bins[min(int(math.log(n, 2)), NUM_BINS - 1)] += 1
    yield zip(LABELS, bins)

def features(profile):
    def all_sessions():
        for session in profile['sessions']:
            yield 'device:%s' % session['dv']
            yield 'version:%s' % session['v']
            for event in session['l']:
                yield 'event:%s' % event['e']
                if 'p' in event and 'Make' in event['p']:
                    yield 'car:%s' % event['p']['Make']
    return frozenset(all_sessions())

def segment(profiles):
    def table(seg, other):
        rows = []
        for feature, count in seg.iteritems():
            if count > COUNT_LIMIT:
                total = count + other[feature]
                rows.append({'feature': feature,
                             'probability': float(count) / total,
                             '# users (total)': total,
                             '# users': count})
        rows.sort(key=lambda x: x['probability'], reverse=True)
        return rows[:TOPN]
        
    stats = (Counter(), Counter())
    segsize = [0, 0]
    for profile in profiles:
        seg = int(sum(1 for s in profile['sessions']) > 1)
        segsize[seg] += 1
        stats[seg].update(features(profile))
    
    text['segsize'] = segsize
    
    for i, label in enumerate(('active (%d users)' % segsize[1],
                               'inactive (%d users)' % segsize[0])):
        yield {'type': 'table',
               'label': label,
               'color': i + 2,
               'csv_export': True,
               'chart': {'probability': 'bar'},
               'size': (12, 6),
               'data': table(stats[i ^ 1], stats[i])}      

Profiles().map(segment).show()

Profiles().map(num_sessions).show('bar',
                                  label='Number of sessions',
                                  size=(12, 3))

Title('Active vs. Inactive')
Description('{segsize[1]} active users, {segsize[0]} inactive users', text)