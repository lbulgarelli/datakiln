"""
Builds table metadata from redcap api
"""

import time
import json
import logging
import re
from pathlib import Path

from redcap import Project

from data_utils.build_json import get_parser

logging.basicConfig(
    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

# subject_id and redcap_event_name are not defined in metadata from redcap
SUBJECT_COL = "subject_id"
SUBJECT = {
    "name" : SUBJECT_COL,
    "description" : 'Subject identifier.',
    "dtype" : {"postgresql":'TEXT'},
    "required" : True
}

REDCAP_COL = "redcap_event_name"
REDCAP = {
    "name" : SUBJECT_COL,
    "description" : 'Redcap event name.',
    "dtype" : {"postgresql":'TEXT'},
    "required" : True
}

def get_metadata(project):
    """Get metadata for project forms.

    :param project: PyCap project.
    :type project: redcap.Project

    :return: Forms metatada.
    :rtype: pandas.DataFrame
    """

    # Retrieve project metadata
    logging.info("REDCAP: RETRIEVING METADATA")
    metadata = project.export_metadata(format_type="df")
    metadata = metadata.loc[metadata.field_type != "descriptive"]

    # Retrieve field names
    field_names = project.export_field_names(format_type="df")
    field_names = field_names.groupby(level=0)['export_field_name'].apply(list)
    metadata["field_names"] = field_names

    # Some field names are not mapped in metadata (signatures)
    # print(metadata[metadata.field_names.isnull()].index)

    return metadata.where(metadata.notnull(), None).copy()


def get_postgres_type(field_type, validation):
    """Define PostgreSQL column types from redcap metadata.

    :param c_type: Python type.
    :type c_type: type

    :return: PostgreSQL data type.
    :rtype: str
    """
    if field_type == "checkbox":
        return "INTEGER[]"
    if validation == "integer":
        return "INTEGER"
    if validation == "number":
        return "DOUBLE PRECISION"
    if validation == "date_dmy":
        return "DATE"
    if validation in ["datetime_dmy", "datetime_seconds_dmy"]:
        return "TIMESTAMP"
    return "TEXT"


def generate_column_meta(field_name, field_meta):
    """Generate column metadata from redcap.

    :param field_name: Name of the redcap form field.
    :type field_name: str

    :param field_meta: PyCap metadata record.
    :type field_meta: pandas.DataFrame

    :return: Column metatada.
    :rtype: dict
    """

    column_meta = {
        "name": field_name,
        "description": field_meta.select_choices_or_calculations,
        "dtype": {
            "postgresql": get_postgres_type(
                field_meta.field_type,
                field_meta.text_validation_type_or_show_slider_number)
                },
        "required": field_meta.required_field == "y"
    }

    # Replace ' for '' standard in PostgreSQL
    if column_meta["description"] is not None:
        column_meta["description"] = column_meta["description"].replace("'","''")

    return column_meta


def generate_table_meta(form_name, form_meta, is_data_empty,
                        primary_key="meta_row_id", 
                        schema_name="", encoding="utf-8"): 
    # pylint: disable=too-many-arguments
    """Generate table metadata from redcap.

    :param form_name: Name of the redcap form.
    :type form_name: str

    :param form_meta: PyCap metadata records.
    :type form_meta: pandas.DataFrame

    :param is_data_empty: Flag identifying if data contain relevant fields.
    :type form_name: boolean

    :param primary_key: Primary key for the table.
    :type schema_name: str
    
    :param schema_name: Name of the schema in PostgreSQL db.
    :type schema_name: str

    :param encoding: Encoding type
    :type encoding: str

    :return: Table metadata.
    :rtype: dict
    """
    table_meta = {
        "name": form_name, 
        "schema": schema_name,
        "encoding": encoding,
        "columns": [],
        "constraints": {"primary_key": [primary_key]}
    }

    # Generate metadata for each column
    for field_name, field_meta in form_meta.iterrows():

        # Ensure RedCap's columns are not present in meta (those are added in generate_table_meta)
        if field_name != SUBJECT_COL and field_name != REDCAP_COL:
            table_meta["columns"].append(generate_column_meta(field_name,field_meta))

    # Add subject and redcap identifiers
    table_meta["columns"].append(SUBJECT)
    table_meta["columns"].append(REDCAP)

    if not is_data_empty:
        # Add form completion column
        table_meta["columns"].append({
            "name": f"{form_name}_complete",
            "dtype": {"postgresql":"TEXT"},
            "required": False, # Should be True, changed to False for one project
            "description": None
        })

    return table_meta


def generate_table_data(project, form_name, form_meta, na_values=None):
    """Retrieve table data from redcap.

    :param project: PyCap project.
    :type project: redcap.Project

    :param form_name: Name of the redcap form.
    :type form_name: str

    :param form_meta: PyCap metadata records.
    :type form_meta: pandas.DataFrame

    :return: Table data.
    :rtype: pandas.DataFrame
    """
    # Retrieve data from REDCap
    logging.info("GENERATING TABLE DATA: %s", form_name)
    data = project.export_records(
        format_type='df', forms=[form_name], decimal_character=".",
        df_kwargs={"index_col": None, "na_values": na_values or []})

    # Get metadata columns names
    meta_columns = form_meta.index.tolist()

    ## Get data column names and remove columns present across all data for comparison
    # Remove columns that don't exist in metadata
    data_columns = data.drop(['subject_id','redcap_event_name',f"{form_name}_complete"],
                    axis='columns',
                    errors='ignore'
                    ).columns.values.tolist() # Ignore errors for _complete columns

    # print(data_columns)
    # Data is empty if it doesn't contain any columns besides the three above
    if data_columns == []:
        is_data_empty = True
    else:
        is_data_empty = False

    # Compare meta and data columns before one hot encoding checkbox fields
    if meta_columns != data_columns:
        diff_columns = list(set(data_columns) - set(meta_columns))

        # Exist in data but not in meta
        if diff_columns:

            # Find checkbox columns in data to validate differences
            r = re.compile(".*___")
            diff_columns_meta = list(filter(r.match, diff_columns))
            diff_columns = list(set(diff_columns_meta) - set(diff_columns))

            # If columns still differ
            if diff_columns:
                logging.warning("DATA AND METADATA FIELDS DIFFER: %s", diff_columns)

        # Exist in meta but not in data
        elif not diff_columns:
            diff_columns = list(set(meta_columns) - set(data_columns))
            logging.warning("METADATA AND DATA FIELDS DIFFER: %s", diff_columns)
            form_meta = form_meta.drop(diff_columns, axis='rows')
            logging.warning("METADA COLUMNS DROPPED SUCESSFULLY.")

    # Checkbox fields are multiple columns, so we group them into an array
    # comorbidity_1, comorbidity_2    to    comorbidity
    #             1,             1               [1, 2]

    dummies = form_meta[form_meta.field_type == "checkbox"]
    for dummy_target, dummy in dummies.iterrows():
        melted = data[dummy.field_names].melt(ignore_index=False)
        codes = melted[melted.value == 1].variable.str.extract(r"(\d+)\Z")
        grouped = codes.astype(int).groupby(level=0, group_keys=False)
        data[dummy_target] = grouped[0].apply(list).astype(str)
        if data[dummy_target].any():
            data[dummy_target] = data[dummy_target].replace(
                {r"\[": "{", r"\]": "}"}, regex=True).fillna("{}")
        data = data.drop(dummy.field_names, axis=1)

    # Columns with integers and null values are parsed to float in pandas
    # so we need to parse it back
    type_col = "text_validation_type_or_show_slider_number"
    integers = form_meta[form_meta[type_col] == "integer"].index
    data[integers] = data[integers].astype("Int64")

    return data, form_meta, is_data_empty


def main(args=None):
    # pylint: disable=too-many-locals
    """Downloads REDCap data and creates sample jsons for tables."""
    # Arguments
    parser = get_parser()
    parser.add_argument('--api-url', metavar="api_url", type=str,
                        required=True, help='REDCap API URL.')
    parser.add_argument('--api-key', metavar="api_key", type=str,
                        required=True, help='REDCap API token.')
    parser.add_argument('--json', action='store_true', default=False,
                        help='Whether to export the JSON table metadata.')
    parser.add_argument('--schema', metavar='schema', type=str,
                        help='Specify the schema name for the json metadata, default is none.')
    parser.add_argument('--include', metavar='include', type=str, nargs='+',
                        help='Specify forms to include, default is all.')
    parser.add_argument('--exclude', metavar='exclude', type=str, nargs='+',
                        help='Specify forms to exclude, default is none.')
    args = parser.parse_args(args)
    directory = Path(args.directory).absolute()

    # API parameters
    logging.info("REDCAP: CONNECTING TO API")
    project = Project(args.api_url, args.api_key)

    # # Get all metatada for each redcap form
    metadata = get_metadata(project)

    # # Export all forms if not specified
    exclude = set(args.exclude or [])
    include = set(args.include or metadata.form_name.unique()) - exclude

    # # Get all data files in directory
    for form_name, form_meta in metadata.groupby("form_name"):

        # Timeout for API handle
        time.sleep(1)

        # Skip if form not specified
        if form_name not in include:
            continue

        # Create data directory if not exists
        data_dir = directory / "data"
        data_dir.mkdir(exist_ok=True)

        # Generate table data
        data, form_meta, is_data_empty = generate_table_data(project, form_name, form_meta)

        # Export to file
        file_path = data_dir / f"{form_name}.csv"
        data.to_csv(file_path, encoding="utf-8", index=False) # Remove index for data_utils

        # Generate table metadata from form
        if args.json:
            logging.info("GENERATING TABLE METADATA: %s", form_name)
            table_meta = generate_table_meta(form_name, form_meta, is_data_empty,
                                            schema_name = args.schema)

            # Create tables directory if not exists
            tables_dir = directory / "metadata"
            tables_dir.mkdir(exist_ok=True)

            # Write json file
            target_fn = f"{tables_dir/form_name}.json"
            with open(target_fn, "w", encoding="utf-8") as target_file:
                json.dump(table_meta, target_file, indent=4)

if __name__ == "__main__":
    main()
