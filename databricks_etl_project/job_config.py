run_data_date = '2024-11-11'

config = {
    "tables": [
        "table1",
        "table2"
    ],
    "ftype":[
        "table",
        "table"
    ],
    "filter_conditions": [
        {"ds_partition_dt": run_data_date, "ds_record_status": 1},
        {"ds_partition_dt": run_data_date, }
    ],
    "table_alias":[
        "a","b"
    ],
    "join_conditions": [
        "a.id = b.id"
    ],
    "join_type":[
        "left",
        "left"
    ],
    "selected_columns":[
        "a.id",
        "b.name"
    ],
    "columns_alias":[
        "id",
        "b"
    ]
}

print(config)
