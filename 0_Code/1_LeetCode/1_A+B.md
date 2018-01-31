> 给出两个整数a和b, 求他们的和, 但不能使用 + 等数学运算符。
```
def aplusb(a, b):  
    if((a&b) == 0):
        return a|b
    return aplusb(a^b,(a&b)<<1)
```
