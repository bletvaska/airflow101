print('>> start')

try:
    1 / 0                     # ZeroDivisionError
    print(name)               # NameError
    open('/etc/passwd', 'w')  # PermissionError

except PermissionError as ex:
    print('--> Nemas pristupove prava')
    print(type(ex))

except NameError as ex:
    print('--> Ta neznas, ze nemas?')
    print(type(ex))

# except Exception as ex:
#     print('--> Tak daco nedobre sa stalo.')
#     print(ex)

print('>> end')