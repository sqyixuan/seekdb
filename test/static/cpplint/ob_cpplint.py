import argparse
import os
import glob
import codecs
import re
import yaml
import sys

_VALID_EXTENSIONS = set(['cc', 'h', 'cpp', 'cu', 'cuh'])

PY_MAJOR_VERSION = sys.version_info.major
if PY_MAJOR_VERSION < 3:
    reload(sys)
    sys.setdefaultencoding('utf8')
    


class CheckCommand:
    
    MSG = ""
    
    @staticmethod
    def remove_comments(contents):
        res = []
        single_comment_pattern = re.compile(r'^\s*//.*$')
        multiline_commend_start_pattern = re.compile(r'^\s*\\\*.*$')
        multiline_commend_end_pattern = re.compile(r'^\s*\*\\.*$')
        clean_for_multiline_comment = False
        for line in contents:
            if clean_for_multiline_comment:
                continue
            if single_comment_pattern.match(line):
                pass
            elif multiline_commend_start_pattern.match(line):
                clean_for_multiline_comment = True
            elif multiline_commend_end_pattern.match(line):
                clean_for_multiline_comment = True
            else:
                res.append(line)
        return res

    
    @classmethod
    def check(cls, file_contents, whitelists=None):
        pass

    @classmethod
    def filter_by_whitelists(cls, file_content, whitelists=None):
        whitelists = whitelists or []
        if not whitelists:
            return False
        else:
            for whitelist in whitelists:
                filter_content = whitelist["content"]
                if filter_content in file_content:
                    print("{} hit the whitelist {}".format(file_content, filter_content))
                    return True                
        return False
        
class CheckCommandWithRe(CheckCommand):
    
    MSG = ""
    ERROR = ""
    CHECK_PATTERNS = ""
    
    @classmethod
    def check(cls, file_contents, whitelists=None):
        msg = ""
        result_contents = []
        res = True
        contents = cls.remove_comments(file_contents)
        current_index = 0
        for file_content in contents:
            for pattern in cls.CHECK_PATTERNS:
                if pattern.search(file_content) and cls.special_judge(file_content):
                    if cls.filter_by_whitelists(file_content, whitelists):
                        break
                    line_index = file_contents.index(file_content, current_index)
                    start_index = line_index - 1 if line_index > 0 else 0
                    end_index = line_index + 2
                    line = line_index + 1
                    context = "\n".join(file_contents[start_index:end_index])
                    result_contents.append(
                        {
                            'error': cls.ERROR,
                            'line': line,
                            "content": file_content,
                            "context": context
                         }
                    )
                    msg += (cls.MSG.format(
                        line=line,
                        content=file_content,
                        context=context
                    )+ "\n")
                    current_index = line_index + 1
                    res = False
                    break
        data = {
            "summary": msg,
            "contents": result_contents
        }
        return res, data
    
    @classmethod
    def special_judge(cls, line):
        return True
    
class CheckAutoCommand(CheckCommandWithRe):
    
    ERROR="auto"
    MSG = "auto check,line: {line},content: {content}\ncontext:\n{context}"
    CHECK_PATTERNS = [re.compile(r'auto\s+[a-zA-Z1-9_]+\s?=')]

class CheckLambdaCommand(CheckCommandWithRe):
    
    ERROR="lambda"
    MSG = "lambda check,line: {line},content: {content}\ncontext:\n{context}"
    CHECK_PATTERNS = [
        re.compile(r'\[[^\[\]]*\]\s*\([^\(\)]*\)\s*->\s*[a-zA-Z1-9_]+\s*\{'),  # check the lambdas with return declartion
        re.compile(r'\[[^\[\]]*\]\s*\([^\(\)]*\)\s*\{')  # check the lambdas without return declartion
    ]
    
    def special_judge(line):
        if "operator[]" in line:
            return False
        return True

check_commands = [
    CheckAutoCommand,
    CheckLambdaCommand
]


def check_file(filename, whitelists=None):
    res = True
    msgs = ""
    check_contents = []
    whitelists = whitelists or []
    file_contents = codecs.open(filename, 'r', 'utf8', 'replace').read().split("\n")
    for check_command in check_commands:
        this_command_whitelists = [
            filter_data for filter_data in whitelists if filter_data["check"] == check_command.ERROR
        ]
        result, this_command_check_data = check_command.check(
            file_contents, whitelists=this_command_whitelists
        )
        if not result:
            res = False
            msg = this_command_check_data["summary"]
            check_contents += this_command_check_data["contents"]
            msgs += "{}".format(msg)
    return res, {"summary": msgs, "contents": check_contents}


def get_white_lists(white_file):
    if white_file is not None:
        if os.path.isfile(white_file):
            try:
                with open(white_file, encoding='utf-8') as f:
                    white_datas = yaml.load(f, Loader=yaml.SafeLoader)
                    return white_datas
            except:
                import traceback
                traceback.print_exc()
                print("invaild whitelist")
                exit(1)
    return {}
        
def get_white_filenames(analyse_dir, need_filter_white_filenames):
    res = []
    os.chdir(analyse_dir or './')
    for line in need_filter_white_filenames:
        if not line.startswith('#'):
            line=line.replace('\n', '')
            if os.path.isfile(line):
                # For files, since walk does not return information, it needs to be judged separately
                res.append(os.path.join(analyse_dir, line))
            elif os.path.isdir(line):
                for dir, _, filenames in os.walk(line):
                    res.extend(
                        [os.path.join(analyse_dir, dir, filename) for filename in filenames]
                            )
    res = set(res)
    return res

def get_need_analyse_files(files=None, analyse_dir=None, whitelists=None, extensions=None):
    white_filenames = []
    if whitelists is not None:
        need_filter_white_filenames = [
            filename for filename, filter_data in whitelists.items() if filter_data == "all"
        ]
        white_filenames = get_white_filenames(analyse_dir, need_filter_white_filenames)
    extensions = extensions or ['cpp', 'h']
    extensions=set(extensions)
    if files is None:
        files = []
        analyse_dir = analyse_dir or './'
        for workdir, _, filenames in os.walk(analyse_dir):
            for filename in filenames:
                files.append(
                    os.path.join(workdir, filename)
                )
    else:
        files = [os.path.join(analyse_dir, filepath) for filepath in files]
    check_files = [
        filename for filename in files if (filename not in white_filenames) and filename.split(".")[-1] in extensions
    ]
    return check_files
                

def analyse_files(files=None, analyse_dir=None, white_file=None, extensions=None):
    res = []
    count = 0
    whitelists = get_white_lists(white_file)
    check_files = get_need_analyse_files(files, analyse_dir, whitelists, extensions=extensions)
    for filename in check_files:
        this_file_whitelists = whitelists.get(filename)
        check_res, check_errors = check_file(filename, this_file_whitelists)
        if not check_res:
            print("file {} has some questions:\n{}".format(
                filename, check_errors["summary"]
            ))
            res.append({"filename": filename, "content": check_errors})
            count += 1
    return res   

def analyse_with_check_status(files=None, analyse_dir=None, white_file=None, extensions=None):
    result = analyse_files(files, analyse_dir, white_file, extensions)
    if result:
        exit(1)

def main():
    parser = argparse.ArgumentParser(description='OB cpplint tool')
    parser.add_argument('-w', '--white-file',dest="white_file", help="whitelist file")
    parser.add_argument('--extensions', action="append", help="the check extsnstions of the file, default will be cpp h")
    parser.add_argument('-f', '--analyse_files', action="append", help="analyse files", default=None)
    parser.add_argument('-d', '--analyse_dir', help="analyse dir", default=None)

    args = parser.parse_args()
    analyse_with_check_status(files=args.analyse_files, analyse_dir=args.analyse_dir, white_file=args.white_file, extensions=args.extensions)
    

if __name__ == '__main__':
    main()