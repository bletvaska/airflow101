#!/usr/bin/env python3

    
def f1():
    print('>> f1()')
    10 / 0
    
def f2():
    print(">> f2()")
    f1()
    
f2()
