def get_foreach_args(scale):
    return dict(elements=[1,2,3,4], extra_args=[float(scale)])
    
def foreach(x, scale):
    return x * scale

def gather(x, *other_args):
    print(x)
