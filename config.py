# {'pattern': r'', 'repl': r''}
spatialite_regex = [
    {'pattern': r'\bTIMESTAMPZ?(?:\s*WITH(?:OUT)? TIME ZONE)?\b', 'repl': r'DATETIME'},
    {'pattern': r'\bBYTEA\b', 'repl': r'BLOB'},
    {'pattern': r'\bUUID\b', 'repl': r'TEXT'},
    {'pattern': r'\bSERIAL\s+PRIMARY\s+KEY\b', 'repl': r'INTEGER PRIMARY KEY AUTOINCREMENT'},
    {'pattern': 'CREATE OR REPLACE VIEW', 'repl': 'CREATE VIEW IF NOT EXISTS'},
    {'pattern': r'(\s+REFERENCES\s+){schema}\.', 'repl': r'\1'},
    {'pattern': r"""POSITION\s*\(\s*('[^']+'|[A-Za-z_\."]+)\s+IN\s+('[^']+'|[A-Za-z_\."]+)\)""",
     'repl': r'INSTR(\1, \2)'},
    {'pattern': r'SUBSTRING\(', 'repl': r'SUBSTR('},
    {'pattern': r"""LEFT\s*\(\s*([A-Za-z"\.]+)\s*,\s*(\d+)\)""", 'repl': r'SUBSTR(\1, 1, \2)'},
    {'pattern': r"""RIGHT\s*\(\s*([A-Za-z"\.]+)\s*,\s*(\d+)\)""", 'repl': r'SUBSTR(\1, -1, \2)'},
    {'pattern': ''.join((r"""ALTER\s+TABLE\s+([A-Za-z{}"_]+)\.([A-Za-z{}"_]+)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS""",
                         r"""\s+)?([A-Za-z{}"_]+)\s+([A-Za-z{}"_]+)\s*\('?([A-Za-z]+)'?\s*,\s*(\d{4})\);?""")),
     'repl': r"""SELECT AddGeometryColumn('\2', '\3', \6, '\5', 2);"""},
    {'pattern': ''.join((r"""CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z"\{\}_]+)\s+ON\s+([A-Za-z"\{\}_]+)?""",
                         r"""\.?([A-Za-z"\{\}_]+)\s+USING\s+GIST\s+\(([^\)]+)\);?""")),
     'repl': r"""SELECT CreateSpatialIndex('\3', '\4');"""},
    {'pattern': r''.join((r"""CREATE\s+INDEX\s+(IF\s+NOT\s+EXISTS\s+)?([A-Za-z"\{\}_]+)\s+ON\s+([A-Za-z"\{\}_]+)?""",
                          r"""\.?([A-Za-z"\{\}_]+)\s+(?:USING\s+[A-Za-z\-]+\s+)?\(([^\)]+)\);?""")),
     'repl': r'CREATE INDEX \1 \2 ON \4 (\5)'}
]

mssql_regex = [
    {'pattern': r'\bTEXT\b', 'repl': r'NVARCHAR(MAX)'},
    {'pattern': r'\b(?:VARCHAR|CHARACTER\s+VARYING)\b', 'repl': r'NVARCHAR'},
    {'pattern': r'\bINTEGER\b', 'repl': r'INT'},
    {'pattern': r'\bDOUBLE\s+PRECISION\b', 'repl': r'FLOAT'},  # FLOAT(53), but default n=53 so just FLOAT
    {'pattern': r'\bBOOLEAN\b', 'repl': r'BIT'},
    {'pattern': r'\bTIMESTAMPZ?(?:\s*WITH(?:OUT)? TIME ZONE)?\b', 'repl': r'DATETIME2(7)'},
    {'pattern': r'\bBYTEA\b', 'repl': r'VARBINARY'},
    {'pattern': r'\bUUID\b', 'repl': r'UNIQUEIDENTIFIER'},
    {'pattern': r'\bSERIAL\s+PRIMARY\s+KEY\b', 'repl': r'INT IDENTITY(1,1) PRIMARY KEY'},
    {'pattern': r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Za-z\{\}\._]+)([^;]+);',
     'repl': r"""IF OBJECT_ID(N'\1', N'U') IS NULL BEGIN\nCREATE TABLE \1 \2; END;"""}
]