elements = [
'AD',
'A.D.',
'AM',
'aM',
'Am',
'am',
'A.M.',
'a.M.',
'A.m.',
'a.m.',
'BC',
'B.C.',
'CC',
'SCC',
'DAY',
'Day',
'day',
'DD',
'DDD',
'DL',
'DS',
'DY',
'Dy',
'dy',
'FF',
'FF1',
'FF2',
'FF3',
'FF4',
'FF5',
'FF6',
'FF7',
'FF8',
'FF9',
'HH',
'HH12',
'HH24',
'IW',
'IYY',
'IY',
'I',
'IYYY',
'MI',
'MM',
'MON',
'Mon',
'mon',
'MONTH',
'Month',
'month',
'PM',
'pm',
'pM',
'Pm',
'P.M.',
'p.m.',
'p.M.',
'P.m.',
'RR',
'RR',
'RRRR',
'SS',
'SSSSS',
'TZH',
'TZM',
'WW',
'W',
'Y,YYY',
'SYYYY',
'YYYY',
'YYY',
'YY',
'Y',
'TZD',
'TZR',
]
timestamp_value = [
"timestamp'9998-12-31 23:59:59.999999999'",
"timestamp'1000-01-01 00:00:00.000000000'",
"timestamp'1992-12-12 12:12:12.123456789'",
"timestamp'1992-12-12 12:12:12.987654321'",
]

timestamp_value_with_various_precisions = [
]

case_head = [
"#owner: jim.wjh\n",
"#owner group: SQL2\n",
"#description: generated cases by gen_case.py\n",
"##tags: otimestamp\n"
]


def wrap_select(input):
    return "select " + str(input) + " R from dual;\n"

def gen_to_char_test_case(file_name):
    my_file = open(file_name, 'w')
    my_cases = []
    for value in timestamp_value_with_various_precisions:
        for elem in elements:
            if elem.find('FF') != -1:
                to_char_item = "to_char(" + value + ", 'SS." + elem + "')"
                my_cases.append(wrap_select(to_char_item))
    my_file.writelines(case_head)
    my_file.writelines(my_cases)


def main():
    #gen data
    for pricision in range(0, 10):
        for v in timestamp_value:
            casted_v = "cast(" + v + " as timestamp(" + str(pricision) + "))"
            timestamp_value_with_various_precisions.append(casted_v);

    gen_to_char_test_case('generated_to_char_oracle.test')


if __name__ == '__main__':
    main()





