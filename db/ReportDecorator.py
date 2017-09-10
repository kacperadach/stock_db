from reporting.report import Reporter

def report(insert_func):
    def reporting_function(*args, **kwargs):
        if isinstance(args[1], dict):
            num_documents = 1
        else:
            num_documents = len(args[1])
        # args[2] should be collection
        Reporter.add_to_report(args[2], num_documents)
        insert_func(*args, **kwargs)
    return reporting_function
