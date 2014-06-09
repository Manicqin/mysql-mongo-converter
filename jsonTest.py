import json

bla = {}


bla = { 'a' : 1 , 's' : 2}
bla['e'] =  {'q' : 1 , 'w' : 2}
print bla

test =  json.dumps(bla)
print test
