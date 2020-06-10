import re

def getConnStr() -> str:
    pattern = r'[ \t]*([a-zA-Z0-9-]*)[ \t]*=[ \t]*([a-zA-Z0-9-+*\/=?!@#$%&()_{}<>:;,.~^`"\' ]*)[ \t]*'
    text = open('keys_test.ini','r').read()

    for m in re.findall(pattern, text):
        if m[0] == 'connectionStringTest':
            return m[1]
    return None
