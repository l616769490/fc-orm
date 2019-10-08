from inspect import isfunction
def aaa():
    return 'aaa'

if __name__ == "__main__":
    distinctStr = True
    strDict = {
            'distinctStr':'distinct' if  distinctStr else '',
            'propertiesStr':'', 
            'fromStr':'',
            'whereStr':'',
            'orderByStr':'',
            'groupByStr':'',
            'limitStr':''
        }
    
    print(strDict)