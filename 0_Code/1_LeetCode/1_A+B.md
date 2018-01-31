```
def aplusb(a, b):  
    if((a&b) == 0):
        return a|b
    return aplusb(a^b,(a&b)<<1)
```
