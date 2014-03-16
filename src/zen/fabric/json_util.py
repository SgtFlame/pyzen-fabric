
def filter_json(obj):
    if type(obj) in (bool, float, int, str):
        return obj
    elif type(obj) == unicode:
        return unicode(obj)
    elif type(obj) == list:
        obj = list(obj)
        for i, v in enumerate(obj):
            obj[i] = filter_json(v)
    elif type(obj) == tuple:
        obj = list(obj)
        for i, v in enumerate(obj):
            obj[i] = filter_json(v)
        obj = tuple(obj)
    elif type(obj) == set:
        obj = list(obj)
        for i, v in enumerate(obj):
            obj[i] = filter_json(v)
        obj = set(obj)
    elif type(obj) == dict:
        newobj = {}
        for i, v in obj.iteritems():
            key = str(i)
            newobj[key] = filter_json(v)
            obj = newobj
    else:
        obj = str(obj)
    return obj
