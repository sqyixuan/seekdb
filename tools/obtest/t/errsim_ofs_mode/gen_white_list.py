#! /usr/bin/env python3
import os
cases = ""
with open("./multi_zone.list", 'r') as multi_zone_white_list:
    for line in multi_zone_white_list:
        line = line.strip()
        if (line.find('#') != -1):
            # print("found comment: " + line)
            pass
        elif (line == ""):
            pass
        elif (cases == ""):
            cases += line
        else:
            cases += "," + line

with open("./white_list", 'w') as result_file:
    result_file.write(cases)

print("result cases: " + cases)

