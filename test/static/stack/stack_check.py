import subprocess
import re
import sys
import argparse

def get_stack_size(lines):
    subq_pattern = r"subq\s\$([0-9]+), %rsp"
    stack_size = 0

    for line in lines:
        sub_match = re.search(subq_pattern, line)
        if sub_match:
            stack_size = int(sub_match.group(1))
            break

    return stack_size

def parse_objdump(max_stack_size):
    function_pattern = r"^[a-f0-9]+ <([^ ]+)>:$"
    lines = sys.stdin.readlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        match = re.match(function_pattern, line)
        if match:
            function_name = match.groups()[0]

            j = i + 1
            while j < len(lines) and not re.match(function_pattern, lines[j]):
                j += 1

            stack_size = get_stack_size(lines[i:j])
            if stack_size > max_stack_size:
              print("{} {}".format(stack_size, function_name))
            i = j
        else:
            i += 1

parser = argparse.ArgumentParser(description='Stack usage analysis.')
parser.add_argument('-l', type=int, nargs=1, metavar='max_stack_size',
                    help='specify the stack size threshold (default 0x2000)')
args = parser.parse_args()
max_stack_size = 0x2000
if args.l:
    max_stack_size = args.l[0]
parse_objdump(max_stack_size)
