from datetime import datetime

REPORTING_COLLECTION = 'reporting'

def report(insert_func):
    def reporting_function(*args, **kwargs):
        original_collection = args[0].collection
        if original_collection != REPORTING_COLLECTION:
            args[0].set_collection(REPORTING_COLLECTION)
            today = str(datetime.now().date())
            existing_report = list(args[0].find({"trading_date": today}))

            if isinstance(args[1], dict):
                num_documents = 1
            else:
                num_documents = len(args[1])

            if len(existing_report) == 0:
                document = {original_collection: num_documents, "trading_date": today}
                args[0].insert_one(document)
            else:
                document = existing_report[0]
                if original_collection in document.keys():
                    document[original_collection] = document[original_collection] + num_documents
                else:
                    document[original_collection] = num_documents
                args[0].save(document)
            args[0].set_collection(original_collection)
        insert_func(*args, **kwargs)
    return reporting_function
