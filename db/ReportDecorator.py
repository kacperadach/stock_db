from reporting.report import Reporter

REPORTING_COLLECTION = 'reporting'

def report(insert_func):
    def reporting_function(*args, **kwargs):
        if isinstance(args[1], dict):
            num_documents = 1
        else:
            num_documents = len(args[1])
        Reporter.add_to_report(args[0].collection, num_documents)
        insert_func(*args, **kwargs)
    return reporting_function
