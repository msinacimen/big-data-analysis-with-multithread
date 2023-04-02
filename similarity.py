def formula(a, b):
    return (a/b)*100
def intersection(a, b):
    intersect = []
    list1 = a.lower().split()
    list2 = b.lower().split()
    for i in list1:
        if i in list2:
            intersect.append(i)
    return intersect
def similar(a, b):
    longerwordcount = max(len(a.split()), len(b.split()))
    if a == b:
        return 100.0
    else:
        return formula(len(intersection(a, b)), longerwordcount)


a = 'Credit reporting credit repair services personal consumer reports'
b = 'Credit card prepaid card'
# print(similar(a, b))
