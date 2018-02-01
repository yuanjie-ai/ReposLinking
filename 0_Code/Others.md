- 10进制转换xx
```python
def dec2base(num, base=2):
    """
    int('11', 10)
    :param num:
    :param base:
    :return: 10进制到2-16进制转换
    """
    mid = []
    num = int(num)
    _base = [str(x) for x in range(10)] + [chr(x) for x in range(ord('A'), ord('A') + 6)]
    while num:
        num, rem = divmod(num, base)
        mid.append(_base[rem])
    return ''.join(mid[::-1])
```
