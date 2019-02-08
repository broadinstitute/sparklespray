def scatter():
    return dict(elements=[1,2,3,4], foreach=foreach)
    
def foreach(x):
    print("Here from foreach", x)
