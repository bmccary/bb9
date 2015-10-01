
import re
import traceback
from csvu import (
        reader_make,
        writer_make,
    )
from csvu.cli import default_arg_parser

BB9_USERNAME     = 'Username'
BB9_AVAILABILITY = 'Availability'

BB9_COLUMN_DFA = re.compile(r"(?P<name>[\w\s-]+) \[Total Pts:(?P<upto> up to)? (?P<pts>\d+)\] \|(?P<id>\d+)")

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
    parser = default_arg_parser(
                    description=description,
                    file0='input',
                    file1='output',
                    dialect0='input',
                    dialect1='output',
                )
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
                        file_or_path=args.file0,
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
                        file_or_path=args.file1,
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
    parser = default_arg_parser(
                    description=description,
                    file0='input',
                    file1='output',
                    dialect0='input',
                    dialect1='output',
                )
    return parser

def to_csv_d(row_g, fieldnames):

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
                        file_or_path=args.file0,
                        dialect=args.dialect0,
                        headless=False,
                    )

        dialect0    = reader_d['dialect']
        fieldnames0 = reader_d['fieldnames']
        reader_g    = reader_d['reader']

        filter_d = to_csv_d(
                            row_g=reader_g,
                            fieldnames=fieldnames0,
                        )

        filter_g    = filter_d['to_csv_g']
        fieldnames1 = filter_d['fieldnames']
        

        dialect1 = args.dialect1

        if dialect1 == 'dialect0':
            dialect1 = dialect0

        writer_f = writer_make(
                        file_or_path=args.file1,
                        dialect=dialect1,
                        headless=False,
                        fieldnames=fieldnames1,
                    )

        writer_f(filter_g)
                        

    except Exception as exc:

        m = traceback.format_exc()
        parser.error(m)
        



def meta_join_arg_parser():
    description = 'BB9 Meta Join takes a CSV file BB9 Meta file and produces a CSV file with the column metadata re-inserted.'
    parser = default_arg_parser(
                    description=description,
                    file0='input',
                    file1='input',
                    file2='output',
                    dialect0='input',
                    dialect1='input',
                    dialect2='output',
                )
    return parser

def meta_join_d(row_g, meta_g, fieldnames):

    fn2meta = {m['name']: serialize_BB9_column_name(m) for m in meta_g}

    fieldnames1 = [fn2meta.get(fn, fn) for fn in fieldnames]

    def g():
        for row in row_g:
            yield {fn2meta.get(k, k): v for k, v in row.iteritems()}

    return {'fieldnames': fieldnames1, 'meta_join_g': g()}
    
def meta_join_program():

    parser = meta_join_arg_parser()

    args = parser.parse_args()

    try:

        reader0_d = reader_make(
                        file_or_path=args.file0,
                        dialect=args.dialect0,
                        headless=False,
                    )

        reader1_d = reader_make(
                        file_or_path=args.file1,
                        dialect=args.dialect1,
                        headless=False,
                    )

        dialect0    = reader0_d['dialect']
        fieldnames0 = reader0_d['fieldnames']
        reader0_g   = reader0_d['reader']

        dialect1    = reader1_d['dialect']
        fieldnames1 = reader1_d['fieldnames']
        reader1_g   = reader1_d['reader']

        filter_d = meta_join_d(
                            row_g=reader0_g,
                            meta_g=reader1_g,
                            fieldnames=fieldnames0,
                        )

        filter_g    = filter_d['meta_join_g']
        fieldnames2 = filter_d['fieldnames']

        dialect2 = args.dialect2

        if dialect2 == 'dialect0':
            dialect2 = dialect0
        elif dialect2 == 'dialect1':
            dialect2 = dialect1

        writer_f = writer_make(
                        file_or_path=args.file2,
                        dialect=dialect2,
                        headless=False,
                        fieldnames=fieldnames2,
                    )

        writer_f(filter_g)
                        

    except Exception as exc:

        m = traceback.format_exc()
        parser.error(m)
        


