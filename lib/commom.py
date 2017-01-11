# !/usr/bin/env python
# coding: utf-8

def splite_list(all_list, divide=2):
    length = len(all_list)
    split = length // divide
    add_1 = length % divide
    begin = 0
    result = []
    for i in range(1, divide + 1):
        if i == divide:
            result.append(all_list[begin:])
        elif i <= add_1:
            result.append(all_list[begin:split + begin + 1])
            begin += split + 1
        else:
            result.append(all_list[begin:split + begin])
            begin += split

    return result
