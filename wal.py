#!/usr/bin/env python3
"""Write-ahead log implementation."""
import sys, json, os, time, struct
class WAL:
    def __init__(self,path='wal.log'): self.path=path; self.seq=0
    def append(self,op,data):
        self.seq+=1
        entry=json.dumps({'seq':self.seq,'op':op,'data':data,'ts':time.time()})+'\n'
        with open(self.path,'a') as f: f.write(entry); f.flush(); os.fsync(f.fileno())
        return self.seq
    def replay(self):
        if not os.path.exists(self.path): return []
        entries=[]
        for line in open(self.path):
            try: entries.append(json.loads(line))
            except: pass
        return entries
    def checkpoint(self,state,state_file='state.json'):
        json.dump(state,open(state_file,'w'))
        os.unlink(self.path) if os.path.exists(self.path) else None
        self.seq=0
# Demo: key-value store with WAL
wal=WAL('/tmp/demo_wal.log')
state={}
# Replay any existing WAL
for entry in wal.replay():
    if entry['op']=='SET': state[entry['data']['key']]=entry['data']['value']
    elif entry['op']=='DEL': state.pop(entry['data']['key'],None)
# New operations
for k,v in [('x',1),('y',2),('z',3)]:
    wal.append('SET',{'key':k,'value':v}); state[k]=v
wal.append('DEL',{'key':'y'}); state.pop('y',None)
print(f"State: {state}")
print(f"WAL entries: {len(wal.replay())}")
wal.checkpoint(state); print("Checkpointed, WAL cleared")
