with open('data/schemas.json', 'r') as f:
    schemas = json.load(f)

# AEMO's schemas have Oracle SQL types
# map those to types polars can use
# e.g. DATE -> pl.datatypes.Date
# NUMBER(2,0) -> pl.Int16
# NUMBER(15,5) -> pl.Float64
# VARCHAR2(10) -> pl.String
def aemo_type_to_polars_type(t: str) -> pl.datatypes.classes.DataTypeClass:
    t = t.upper()
    if re.match(r"VARCHAR(2)?\(\d+\)", t):
        return pl.String
    if re.match(r"CHAR\((\d+)\)", t):
        # single character
        # polars has no dedicated type for that
        # so use string
        # (could use categorical?)
        return pl.String
    elif t.startswith("NUMBER"):
        match = re.match(r"NUMBER ?\((\d+), ?(\d+)\)", t)
        if match:
            whole_digits = int(match.group(1))
            decimal_digits = int(match.group(2))
        else:
            # e.g. NUMBER(2)
            match = re.match(r"NUMBER ?\((\d+)", t)
            assert match, f"Unsure how to cast {t} to polars type"
            whole_digits = int(match.group(1))
            decimal_digits = 0
            
        if decimal_digits == 0:
            # integer
            # we assume signed (can't tell unsigned from the schema)
            # but how many bits?
            max_val = 10**whole_digits

            if 2**(8-1) > max_val:
                return pl.Int8
            elif 2**(16-1) > max_val:
                return pl.Int16
            elif 2**(16-1) > max_val:
                return pl.Int16
            elif 2**(32-1) > max_val:
                return pl.Int32
            else:
                return pl.Int64
        else:
            # choose 64 over 32 because everything in R is a double anyway
            # In polars, Decimal is still in experimental mode
            return pl.Float64
    elif (t == 'DATE') or re.match(r"TIMESTAMP\((\d)\)", t):
        # watch out, when AEMO say "date" they mean "datetime"
        # for both dates and datetimes they say "date",
        # but both have a time component. (For actual dates, it's always midnight.)
        return pl.Datetime
    else:
        raise ValueError(f"Unsure how to convert AEMO type {t} to polars type")

for (table, schema) in schemas.items():
    for (col_name, aemo_type) in schema['columns'].items():
        polars_type = aemo_type_to_polars_type(aemo_type)
        schema['columns'][col_name] = {
            'aemo_type': aemo_type,
            'polars_type': polars_type,
        }
    schema['columns']['SCHEMA_VERSION'] = {
        'polars_type': pl.UInt8
    }