import inspect

cur_file = ''
def get_caller_info():
    try:
        frame, filename, lineno, function, code_ctx, index = inspect.stack(1)[2]
        if code_ctx == None:
            code = cur_file.split('\n')[lineno - 1]
        else:
            code = code_ctx[0]
        return dict(filename=filename, lineno=lineno, function=function, code=code, locals=frame.f_locals, globals=frame.f_globals)
    finally:
        del frame

