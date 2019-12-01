ENVIRONMENTS = ('dev', 'prod')

def database_test(env='prod'):
    if env not in ENVIRONMENTS:
        raise AssertionError('dev or prod asshole')
    import time
    from db.Finance import FinanceDB
    fdb = FinanceDB()

    test_collection= 'test'

    expected_document = {'time': time.time()}
    fdb.replace_one(test_collection, {}, expected_document, upsert=True)

    documents = list(fdb.find(test_collection, expected_document, {'time': 1}))
    if len(documents) == 0:
        raise AssertionError ('document not found')

    found_document = documents[0]
    if (found_document != expected_document):
        raise AssertionError('not the same document')

if __name__ == "__main__":
    import sys
    program_arguments = sys.argv
    env = 'dev'
    if len(program_arguments) > 1:
        for arg in program_arguments[1:]:
            if arg.replace('-', '') in ('dev', 'prod'):
                env = 'prod'
    database_test(env)