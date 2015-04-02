
import re
import traceback
from csvu import (
        default_arg_parser,
        reader_make,
        writer_make,
    )

BB9_USERNAME     = 'Username'
BB9_AVAILABILITY = 'Availability'

BB9_COLUMN_DFA = re.compile(r"(?P<name>[\w\s]+) \[Total Pts:(?P<upto> up to)? (?P<pts>\d+)\] \|(?P<id>\d+)")

BB9_META_FIELDNAMES = [
        'name',
        'upto',
        'pts',
        'id',
    ]

def parse_BB9_column_name(x):
    m = BB9_COLUMN_DFA.match(x)
    if m:
        return m.groupdict()
    return {'name' : x, 'upto' : None, 'pts' : None, 'id' : None}

BB9_COLUMN_FMT_1 = "{name}"
BB9_COLUMN_FMT_2 = "{name} [Total Pts: {pts}] |{id}"
BB9_COLUMN_FMT_3 = "{name} [Total Pts: up to {pts}] |{id}"

def serialize_BB9_column_name(x):
    fmt = BB9_COLUMN_FMT_1
    if x['id']:
        if x['upto']:
            fmt = BB9_COLUMN_FMT_3
        else:
            fmt = BB9_COLUMN_FMT_2
    return fmt.format(**x)

def bb9_row_f(availability_k=BB9_AVAILABILITY, demo=True):

    def availability_f(row):
        return row[availability_k].strip().upper() == 'YES'

    def demo_f(row):
        name = row.get(BB9_USERNAME)
        if name:
            return not name.upper().startswith('BB_DEMO_')
        return True

    fs = []

    if availability_k:
        fs.append(availability_f)

    if demo:
        fs.append(demo_f)

    def f(row):
        return all(fi(row) for fi in fs)        

    return f












def to_meta_arg_parser():
    description = 'BB9 To Meta takes a BB9 CSV download and produces a CSV file full of meta information.'
    parser = default_arg_parser(description)
    return parser

def to_meta_g(fieldnames):

    def g():
        for fn in fieldnames:
            yield parse_BB9_column_name(fn)

    return {'fieldnames': BB9_META_FIELDNAMES, 'to_meta_g': g()}
    
def to_meta_program():

    parser = to_meta_arg_parser()

    args = parser.parse_args()

    try:

        reader_d = reader_make(
                        fname=args.file0,
                        dialect=args.dialect0,
                        headless=False, # FIXME
                    )

        dialect0    = reader_d['dialect']
        fieldnames0 = reader_d['fieldnames']
        reader_g    = reader_d['reader']

        filter_d = to_meta_g(
                            fieldnames=fieldnames0,
                        )

        filter_g    = filter_d['to_meta_g']
        fieldnames1 = filter_d['fieldnames']
        

        dialect1 = args.dialect1

        if dialect1 == 'dialect0':
            dialect1 = dialect0

        writer_f = writer_make(
                        fname=args.file1,
                        dialect=dialect1,
                        headless=False,
                        fieldnames=fieldnames1,
                    )

        writer_f(filter_g)
                        

    except Exception as exc:

        m = traceback.format_exc()
        parser.error(m)
        





def to_csv_arg_parser():
    description = 'BB9 To CSV takes a BB9 CSV file and produces a CSV file stripped of the meta information in the columns.'
    parser = default_arg_parser(description)
    return parser

def to_csv_g(row_g, fieldnames):

    fn02fn1 = {}
    parsed  = []
    for fn in fieldnames:
        p = parse_BB9_column_name(fn)['name']
        fn02fn1[fn] = p
        parsed.append(p)

    fieldnames1 = []
    fieldnames1_set = set()
    for p in parsed:
        if p in fieldnames1_set:
            raise Exception("Duplicate column: {}".format(p))
        fieldnames1.append(p)
        fieldnames1_set.add(p)

    def g():
        f = bb9_row_f()
        for row in row_g:
            if f(row):
                yield {fn02fn1[k]: v for k, v in row.iteritems()}

    return {'fieldnames': fieldnames1, 'to_csv_g': g()}
    
def to_csv_program():

    parser = to_csv_arg_parser()

    args = parser.parse_args()

    try:

        reader_d = reader_make(
                        fname=args.file0,
                        dialect=args.dialect0,
                        headless=False, # FIXME
                    )

        dialect0    = reader_d['dialect']
        fieldnames0 = reader_d['fieldnames']
        reader_g    = reader_d['reader']

        filter_d = to_csv_g(
                            row_g=reader_g,
                            fieldnames=fieldnames0,
                        )

        filter_g    = filter_d['to_csv_g']
        fieldnames1 = filter_d['fieldnames']
        

        dialect1 = args.dialect1

        if dialect1 == 'dialect0':
            dialect1 = dialect0

        writer_f = writer_make(
                        fname=args.file1,
                        dialect=dialect1,
                        headless=False,
                        fieldnames=fieldnames1,
                    )

        writer_f(filter_g)
                        

    except Exception as exc:

        m = traceback.format_exc()
        parser.error(m)
        


