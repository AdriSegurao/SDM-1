#! /usr/bin/env python3

import argparse
import csv
import os
import re
import time
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Tuple, Union

from lxml import etree

__author__ = 'Thom Hurks'

TARGET_ELEMENTS = {"article", "inproceedings", "proceedings"}
PROGRESS_EVERY = 50000


class InvalidElementName(Exception):
    def __init__(self, invalid_element_name, tag_name, parent_name):
        self.invalid_element_name = invalid_element_name
        self.tag_name = tag_name
        self.parent_name = parent_name

    def __str__(self):
        return 'Invalid name %s found in tag %s within element %s' % (
            repr(self.invalid_element_name),
            repr(self.tag_name),
            repr(self.parent_name)
        )


def log(message: str) -> None:
    print(message, flush=True)


def valid_relation(relation: str) -> tuple:
    parts = [part for part in relation.split(':') if len(part) > 0]
    if len(parts) == 2:
        return tuple(parts)
    else:
        raise argparse.ArgumentTypeError('%s must have the form attribute:relation' % relation)


def parse_args():
    parser = argparse.ArgumentParser(description='Parse the DBLP XML file and convert it to CSV')

    parser.add_argument(
        '--xml-filename',
        action='store',
        type=str,
        default='dblp.xml',
        help='XML filename inside data/xml_dblp_data'
    )
    parser.add_argument(
        '--dtd-filename',
        action='store',
        type=str,
        default='dblp.dtd',
        help='DTD filename inside data/xml_dblp_data'
    )
    parser.add_argument(
        '--outputfile',
        action='store',
        type=str,
        default='output.csv',
        help='Base output CSV filename inside data/csv_dblp_data'
    )
    parser.add_argument(
        '--annotate',
        action='store_true',
        required=False,
        help='Write a separate annotated header with type information'
    )
    parser.add_argument(
        '--neo4j',
        action='store_true',
        required=False,
        help='Headers become more Neo4J-specific and a neo4j-import shell script is generated for easy importing. '
             'Implies --annotate.'
    )
    parser.add_argument(
        '--relations',
        action='store',
        required=False,
        type=valid_relation,
        nargs='+',
        help='The element attributes that will be treated as elements, and to which a relation from '
             'the parent element will be created. Example: "author:authored_by".'
    )

    parsed_args = parser.parse_args()

    if parsed_args.neo4j and not parsed_args.annotate:
        parsed_args.annotate = True
        log('--neo4j implies --annotate!')

    if parsed_args.relations:
        attr_rel = {attribute: relation for (attribute, relation) in parsed_args.relations}
        attributes = attr_rel.keys()
        if len(attributes) == len(set(attr_rel.values())) == len(parsed_args.relations):
            parsed_args.relations = attr_rel
            log('Will create relations for attribute(s): %s' % (', '.join(sorted(attributes))))
        else:
            log('error: argument --relations: The element attributes and relation names must be unique.')
            exit(1)
    else:
        parsed_args.relations = dict()

    return parsed_args


def get_project_paths():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent

    input_dir = project_root / 'data' / 'xml_dblp_data'
    output_dir = project_root / 'data' / 'csv_dblp_data'

    output_dir.mkdir(parents=True, exist_ok=True)

    return script_dir, project_root, input_dir, output_dir


def get_elements(dtd_file) -> set:
    dtd = etree.DTD(dtd_file)
    elements = set()
    total_dtd_elements = 0

    for el in dtd.iterelements():
        if el.type == 'element':
            elements.add(el.name)
            total_dtd_elements += 1

    elements.discard('dblp')
    elements = elements.intersection(TARGET_ELEMENTS)

    log(f'DTD elements found: {total_dtd_elements}')
    return elements


def open_outputfiles(elements: set, element_attributes: dict, output_filename: str, annotated: bool = False) -> dict:
    (path, ext) = os.path.splitext(output_filename)
    output_files = dict()

    for element in sorted(elements):
        fieldnames = element_attributes.get(element, None)
        if fieldnames is not None and len(fieldnames) > 0:
            fieldnames = sorted(list(fieldnames))
            fieldnames.insert(0, 'id')
            output_path = '%s_%s%s' % (path, element, ext)

            log(f'Opening output file for {element}: {output_path}')
            output_file = open(output_path, mode='w', encoding='UTF-8', newline='')
            output_writer = csv.DictWriter(
                output_file,
                fieldnames=fieldnames,
                delimiter=';',
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                doublequote=True,
                restval='',
                extrasaction='raise'
            )
            if not annotated:
                output_writer.writeheader()
            output_files[element] = output_writer

    return output_files


def get_element_attributes(xml_file, elements: set) -> dict:
    context = etree.iterparse(
        xml_file,
        dtd_validation=True,
        events=('start', 'end'),
        attribute_defaults=True,
        load_dtd=True
    )
    context = iter(context)
    event, root = next(context)

    data = {element: set() for element in elements}
    current_tag = None
    processed_events = 0
    top_level_counts = {element: 0 for element in elements}

    for event, elem in context:
        processed_events += 1

        if current_tag is None and event == 'start' and elem.tag in elements:
            current_tag = elem.tag
            top_level_counts[current_tag] += 1
            keys = elem.keys()
            if len(keys) > 0:
                data[current_tag].update(set(keys))

            if top_level_counts[current_tag] % PROGRESS_EVERY == 0:
                log(f'Attribute scan progress for {current_tag}: {top_level_counts[current_tag]} elements')

        elif current_tag is not None and event == 'end':
            if elem.tag == current_tag:
                current_tag = None
            elif elem.tag is not None and elem.text is not None:
                if elem.tag == 'id':
                    raise InvalidElementName('id', elem.tag, current_tag)
                attributes = data[current_tag]
                attributes.add(elem.tag)
                keys = elem.keys()
                if len(keys) > 0:
                    for key in keys:
                        attributes.add('%s-%s' % (elem.tag, key))
            root.clear()

        if processed_events % (PROGRESS_EVERY * 4) == 0:
            log(f'Attribute discovery events processed: {processed_events}')

    empty_elements = [element for element, attrs in data.items() if len(attrs) == 0]
    for element in empty_elements:
        data.pop(element)

    for element in data:
        if 'id' in data[element]:
            raise InvalidElementName('id', element, 'root')

    log('Finished attribute discovery.')
    for element in sorted(data.keys()):
        log(f'  {element}: {len(data[element])} attributes discovered')

    return data


def parse_xml(xml_file, elements: set, output_files: Dict[str, csv.DictWriter], relation_attributes: set,
              annotate: bool = False) -> Union[Tuple[dict, int, dict, dict], Tuple[dict, int]]:
    context = etree.iterparse(xml_file, dtd_validation=True, events=('start', 'end'))
    context = iter(context)
    event, root = next(context)

    data = dict()
    relations = dict()
    current_tag = None
    multiple_valued_cells = set()
    unique_id = 0
    processed_events = 0
    element_counts = {element: 0 for element in elements}

    if annotate:
        array_elements = dict()
        element_types = dict()

    for event, elem in context:
        processed_events += 1

        if current_tag is None and event == 'start' and elem.tag in elements:
            current_tag = elem.tag
            data.clear()
            multiple_valued_cells.clear()
            data.update(elem.attrib)

            if annotate:
                for (key, value) in elem.attrib.items():
                    set_type_information(element_types, current_tag, key, value)

        elif current_tag is not None and event == 'end':
            if elem.tag == current_tag:
                if len(data) > 0:
                    set_relation_values(relations, data, relation_attributes, unique_id)
                    for cell in multiple_valued_cells:
                        data[cell] = '|'.join(sorted(data[cell]))
                    data['id'] = unique_id
                    output_files[current_tag].writerow(data)

                    if annotate and len(multiple_valued_cells) > 0:
                        element_cells = array_elements.get(current_tag)
                        if element_cells is None:
                            array_elements[current_tag] = multiple_valued_cells.copy()
                        else:
                            element_cells.update(multiple_valued_cells)

                    element_counts[current_tag] += 1
                    if element_counts[current_tag] % PROGRESS_EVERY == 0:
                        log(f'Parsed {element_counts[current_tag]} rows for {current_tag}')

                    unique_id += 1
                current_tag = None

            elif elem.tag is not None and elem.text is not None:
                set_cell_value(data, elem.tag, elem.text, multiple_valued_cells)

                if annotate:
                    set_type_information(element_types, current_tag, elem.tag, elem.text)

                for (key, value) in elem.attrib.items():
                    column_name = '%s-%s' % (elem.tag, key)
                    set_cell_value(data, column_name, value, multiple_valued_cells)
                    if annotate:
                        set_type_information(element_types, current_tag, column_name, value)

            root.clear()

        if processed_events % (PROGRESS_EVERY * 4) == 0:
            log(f'XML parsing events processed: {processed_events} | rows written so far: {unique_id}')

    log('Finished XML parsing.')
    for element in sorted(element_counts.keys()):
        log(f'  {element}: {element_counts[element]} rows written')

    if annotate:
        return relations, unique_id, array_elements, element_types
    else:
        return relations, unique_id


def set_relation_values(relations: dict, data: dict, relation_attributes: set, to_id: int):
    if len(relation_attributes) == 0:
        return

    for column_name, attributes in data.items():
        if column_name in relation_attributes:
            relation = relations.get(column_name, dict())
            if isinstance(attributes, list):
                for attribute in attributes:
                    rel_instance = relation.get(attribute, set())
                    rel_instance.add(to_id)
                    relation[attribute] = rel_instance
            else:
                rel_instance = relation.get(attributes, set())
                rel_instance.add(to_id)
                relation[attributes] = rel_instance
            relations[column_name] = relation


def set_cell_value(data: dict, column_name: str, value: str, multiple_valued_cells: set):
    entry = data.get(column_name)
    if entry is None:
        data[column_name] = value
    else:
        if isinstance(entry, list):
            entry.append(value)
        else:
            data[column_name] = [entry, value]
            multiple_valued_cells.add(column_name)


def set_type_information(element_types: dict, current_tag: str, column_name: str, value: str):
    attribute_types = element_types.get(current_tag)
    if attribute_types is None:
        element_types[current_tag] = attribute_types = dict()
    types = attribute_types.get(column_name)
    if types is None:
        attribute_types[column_name] = types = set()
    types.add(get_type(value))


def get_type(string_value: str) -> str:
    if string_value is None or len(string_value) == 0:
        return 'any'
    if str.isdigit(string_value):
        try:
            int(string_value)
            return 'integer'
        except ValueError:
            return 'string'
    if get_type.re_number.fullmatch(string_value) is not None:
        try:
            float(string_value)
            return 'float'
        except ValueError:
            return 'string'
    if get_type.re_date.fullmatch(string_value) is not None:
        try:
            date.fromisoformat(string_value)
            return 'date'
        except ValueError:
            return 'string'
    if get_type.re_datetime.fullmatch(string_value) is not None:
        try:
            datetime.fromisoformat(string_value)
            return 'datetime'
        except ValueError:
            return 'string'
    if string_value.lower() == 'true' or string_value.lower() == 'false':
        return 'boolean'
    return 'string'


get_type.re_number = re.compile(r'^\d+\.\d+$')
get_type.re_date = re.compile(r'^\d{4}-\d{2}-\d{2}$')
get_type.re_datetime = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?$')


def write_annotated_header(array_elements: dict, element_types: dict, output_filename: str, neo4j_style: bool = False):
    (path, ext) = os.path.splitext(output_filename)
    for element, column_types in element_types.items():
        if element not in TARGET_ELEMENTS:
            continue

        output_path = '%s_%s_header%s' % (path, element, ext)
        log(f'Writing annotated header for {element}: {output_path}')

        header = []
        array_columns = array_elements.get(element, set())
        columns = sorted(list(column_types.keys()))

        if neo4j_style:
            header.append('%s:ID' % element)
        else:
            columns.insert(0, 'id')
            column_types['id'] = {'integer'}

        for column in columns:
            types = column_types[column]
            high_level_type = get_high_level_type(types)
            typename = translate_type(high_level_type, neo4j_style)
            if column in array_columns:
                header.append('%s:%s[]' % (column, typename))
            else:
                header.append('%s:%s' % (column, typename))

        with open(output_path, mode='w', encoding='UTF-8') as output_file:
            output_file.write(';'.join(header))


def translate_type(type_input: str, neo4j_style: bool = False) -> str:
    if neo4j_style and type_input == 'integer':
        return 'int'
    return type_input


def get_high_level_type(types: set) -> str:
    if len(types) == 0:
        raise Exception('Empty type set encountered', types)

    types.discard('any')

    if len(types) == 0:
        return 'string'
    elif len(types) == 1:
        (high_level_type,) = types
        return high_level_type
    else:
        if 'string' in types:
            return 'string'
        elif len(types) == 2:
            if 'float' in types and 'integer' in types:
                return 'float'
            elif 'date' in types and 'datetime' in types:
                return 'datetime'
    return 'string'


def generate_neo4j_import_command(elements: set, relations: set, relation_alias: dict, output_filename: str):
    (path, ext) = os.path.splitext(output_filename)
    command = 'neo4j-admin import --mode=csv --database=dblp.db --delimiter ";" --array-delimiter "|" --id-type INTEGER'
    for element in sorted(elements.intersection(TARGET_ELEMENTS)):
        command += ' --nodes:%s "%s_%s_header%s,%s_%s%s"' % (element, path, element, ext, path, element, ext)
    for relation in relations:
        command += ' --nodes:%s "%s_%s%s"' % (relation, path, relation, ext)
        predicate = relation_alias[relation]
        command += ' --relationships:%s "%s_%s_%s%s"' % (predicate, path, relation, predicate, ext)
    return command


def write_relation_files(output_filename: str, relations: dict, relation_alias: dict, unique_id: int):
    (path, ext) = os.path.splitext(output_filename)
    total_relation_types = len(relations)
    current_relation_type = 0

    for column_name, relation in relations.items():
        current_relation_type += 1
        output_path_node = '%s_%s%s' % (path, column_name, ext)
        output_path_relation = '%s_%s_%s%s' % (path, column_name, relation_alias[column_name], ext)

        log(f'Writing relation files for {column_name} ({current_relation_type}/{total_relation_types})')

        with open(output_path_relation, mode='w', encoding='UTF-8') as output_file_relation:
            output_file_relation.write(':START_ID;:END_ID\n')
            with open(output_path_node, mode='w', encoding='UTF-8', newline='') as output_file_node:
                node_output_writer = csv.writer(
                    output_file_node,
                    delimiter=';',
                    quoting=csv.QUOTE_MINIMAL,
                    quotechar='"',
                    doublequote=True
                )
                output_file_node.write(':ID;%s:string\n' % column_name)

                written_values = 0
                for value, rel_instance in relation.items():
                    node_output_writer.writerow([unique_id, value])
                    for from_id in rel_instance:
                        output_file_relation.write('%d;%d\n' % (from_id, unique_id))
                    unique_id += 1
                    written_values += 1

                    if written_values % PROGRESS_EVERY == 0:
                        log(f'  {column_name}: {written_values} relation nodes written')


def main():
    args = parse_args()
    start_time = time.time()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    input_dir = project_root / 'data' / 'xml_dblp_data'
    output_dir = project_root / 'data' / 'csv_dblp_data'
    output_dir.mkdir(parents=True, exist_ok=True)

    xml_path = input_dir / args.xml_filename
    dtd_path = input_dir / args.dtd_filename
    output_path = output_dir / args.outputfile

    log('Start!')
    log(f'Script directory: {script_dir}')
    log(f'Project root: {project_root}')
    log(f'Input directory: {input_dir}')
    log(f'Output directory: {output_dir}')
    log(f'XML path: {xml_path}')
    log(f'DTD path: {dtd_path}')
    log(f'Output base path: {output_path}')
    log(f'Target elements: {sorted(TARGET_ELEMENTS)}')

    if not xml_path.is_file():
        log(f'Invalid input XML file: {xml_path}')
        exit(1)

    if not dtd_path.is_file():
        log(f'Invalid input DTD file: {dtd_path}')
        exit(1)

    with open(dtd_path, mode='rb') as dtd_file:
        log('Reading elements from DTD file...')
        elements = get_elements(dtd_file)

    log(f'Elements that will be exported: {sorted(elements)}')

    with open(xml_path, mode='rb') as xml_file:
        log('Finding unique attributes for selected elements...')
        try:
            element_attributes = get_element_attributes(xml_file, elements)
        except InvalidElementName as e:
            log(str(e))
            exit(1)

    log('Opening output files...')
    output_files = open_outputfiles(elements, element_attributes, str(output_path), args.annotate)

    array_elements = None
    element_types = None

    with open(xml_path, mode='rb') as xml_file:
        log('Parsing XML and writing selected CSV files...')
        relation_attributes = set(args.relations.keys())

        if args.annotate:
            (relations, unique_id, array_elements, element_types) = parse_xml(
                xml_file,
                elements,
                output_files,
                relation_attributes,
                annotate=True
            )
        else:
            relations, unique_id = parse_xml(xml_file, elements, output_files, relation_attributes)

    log(f'Total written rows: {unique_id}')

    if args.relations and relations and unique_id >= 0:
        log('Writing relation files...')
        write_relation_files(str(output_path), relations, args.relations, unique_id)

    if args.annotate and array_elements and element_types:
        log('Writing annotated headers...')
        write_annotated_header(array_elements, element_types, str(output_path), args.neo4j)

        if args.neo4j:
            log('Generating neo4j-import command...')
            command = generate_neo4j_import_command(
                set(element_types.keys()),
                set(relations.keys()),
                args.relations,
                str(output_path)
            )

            shell_script_path = output_dir / 'neo4j_import.sh'
            log(f'Writing neo4j-import command to shell script file: {shell_script_path}')
            with open(shell_script_path, mode='w', encoding='UTF-8') as command_file:
                command_file.write('#!/bin/bash\n')
                command_file.write(command)

    end_time = time.time()
    log('Done after %f seconds' % (end_time - start_time))


if __name__ == '__main__':
    main()