import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        addr=data["result"][k]["addresses"]
        print(data["result"][k]["id"],",",data["result"][k]["category"]
        ,",",data["result"][k]["coin"]
        ,",",data["result"][k]["status"],end='')
        for a in addr:
            print (",",a,end='')
        print("")
