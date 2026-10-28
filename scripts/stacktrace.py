def f1():
    print('>> f1()')
    10 / 0   # ZeroDivisionError

def f2():
    print('>> f2()')
    f1()

def f3():
    print('>> f3()')
    f2()

f3()