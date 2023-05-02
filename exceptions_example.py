#!/usr/bin/env python3

print('>> boot')

try:
    print('>> start')
    raise Exception('to si necakal toto')
    # 10 / 0
    print('>> phase 1')
    file = open('/root/xxx.jpg', 'r')
    print('>> end')

# catch
except PermissionError as ex:
    print('Error: Nedostatocne prava.')
    print(ex)
    
except ZeroDivisionError as ex:
    print('Error: ktosi delil nulou a to sa neda')
    print(ex)
    
except Exception as ex:
    print(ex)

print('>> quit')
