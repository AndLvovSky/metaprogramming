import logging
import utils as test_utils

from py2sqlm import Py2SQL
from py2sqlm.fields import *
from py2sqlm.table import table

logging.basicConfig(level='DEBUG')


@table
class GeoInfo:
    id = IntField(primary_key=True)
    area = FloatField()
    tags = JsonbField()

    def __init__(self, id, area, tags):
        self.id = id
        self.area = area
        self.tags = tags


@table
class Person:
    id = IntField(primary_key=True)
    name = TextField()
    city_id = IntField()

    def __init__(self, id, name, city_id):
        self.id = id
        self.name = name
        self.city_id = city_id


@table
class City:
    id = IntField(primary_key=True)
    name = TextField(100)
    capital = BoolField()
    geo_info = ForeignKey(GeoInfo)
    geo_info_new = ForeignKey(GeoInfo, mapping_column='geo_info_new_id')
    citizens = ManyRelation(Person)

    def __init__(self, id, name, capital, geo_info, geo_info_new, citizens):
        self.id = id
        self.name = name
        self.capital = capital
        self.geo_info = geo_info
        self.geo_info_new = geo_info_new
        self.citizens = citizens


if __name__ == '__main__':
    db_config = {
        'host': 'localhost',
        'database': 'country',
        'user': 'postgres',
        'password': 'password',
        'port': 5432
    }
    test_utils.drop_all_tables(db_config)

    py2sql = Py2SQL()
    py2sql.db_connect(**db_config)

    db_engine = py2sql.db_engine
    logging.info(f'Database version: {py2sql.db_engine}')
    assert 'PostgreSQL' in db_engine

    db_name = py2sql.db_name
    logging.info(f'Database name: {db_name}')
    assert db_name == 'country'

    db_tables = py2sql.db_tables
    logging.info(f'Database tables: {db_tables}')
    assert db_tables == []

    py2sql.save_class(GeoInfo)

    db_tables = py2sql.db_tables
    logging.info(f'Database tables: {db_tables}')
    assert db_tables == ['geo_info']

    db_table_structure = py2sql.db_table_structure('geo_info')
    logging.info(f'Table geo_info structure: {db_table_structure}')
    assert test_utils.table_structure_matches({('id', 'bigint'), ('area', 'real'), ('tags', 'jsonb')}, db_table_structure)

    test_utils.execute(db_config, """
      alter table geo_info
         add column extracol1 bigint,
         add column extracol2 bigint;
      alter table geo_info
         drop column area,
         drop column tags
   """)
    db_table_structure = py2sql.db_table_structure('geo_info')
    logging.info(f'Table geo_info structure: {db_table_structure}')
    assert test_utils.table_structure_matches({('id', 'bigint'), ('extracol1', 'bigint'), ('extracol2', 'bigint')},
                                              db_table_structure)

    py2sql.save_class(GeoInfo)

    db_table_structure = py2sql.db_table_structure('geo_info')
    logging.info(f'Table geo_info structure: {db_table_structure}')
    assert test_utils.table_structure_matches({('id', 'bigint'), ('area', 'real'), ('tags', 'jsonb')}, db_table_structure)

    geo_info = GeoInfo(5, None, {'density': 75, 'high': True})
    py2sql.save_object(geo_info)

    geo_info_select = test_utils.get_table_records(db_config, 'geo_info', ['id', 'area', 'tags'])
    logging.info(f'Geo info records: {geo_info_select}')
    assert len(geo_info_select) == 1
    selected_geo_info = geo_info_select[0]
    assert selected_geo_info[0] == 5
    assert selected_geo_info[1] == None
    assert selected_geo_info[2] == {'density': 75, 'high': True}

    py2sql.delete_object(geo_info)

    geo_info_select = test_utils.get_table_records(db_config, 'geo_info', ['id'])
    logging.info(f'Geo info records: {geo_info_select}')
    assert geo_info_select == []

    py2sql.delete_class(GeoInfo)

    db_tables = py2sql.db_tables
    logging.info(f'Database tables: {db_tables}')
    assert db_tables == []

    py2sql.save_hierarchy(City)

    db_tables = py2sql.db_tables
    logging.info(f'Database tables: {db_tables}')
    assert db_tables == ['city', 'geo_info']

    db_table_structure = py2sql.db_table_structure('city')
    logging.info(f'Table city structure: {db_table_structure}')
    expected_columns = {('id', 'bigint'), ('name', 'character varying'), ('capital', 'boolean'), ('geo_info_id', 'bigint'), ('geo_info_new_id', 'bigint')}
    assert test_utils.table_structure_matches(expected_columns, db_table_structure)

    db_table_structure = py2sql.db_table_structure('geo_info')
    logging.info(f'Table geo_info structure: {db_table_structure}')
    assert test_utils.table_structure_matches({('id', 'bigint'), ('area', 'real'), ('tags', 'jsonb')}, db_table_structure)

    py2sql.save_class(Person)

    geo_info = GeoInfo(5, 32, {'density': 75, 'high': True})
    citizens = [Person(1, 'adam', 123), Person(2, 'craig', 24)]
    city = City(123, 'Florence', False, geo_info, None, citizens)
    py2sql.save_object(city)

    city_select = test_utils.get_table_records(db_config, 'city', ['id', 'name', 'capital', 'geo_info_id', 'geo_info_new_id'])
    logging.info(f'City records: {city_select}')
    assert len(city_select) == 1
    selected_city = city_select[0]
    assert selected_city[0] == 123
    assert selected_city[1] == 'Florence'
    assert selected_city[2] == False
    assert selected_city[3] == 5
    assert selected_city[4] == None

    geo_info_select = test_utils.get_table_records(db_config, 'geo_info', ['id', 'area', 'tags'])
    logging.info(f'Geo info records: {geo_info_select}')
    assert len(geo_info_select) == 1
    selected_geo_info = geo_info_select[0]
    assert selected_geo_info[0] == 5
    assert selected_geo_info[1] == 32.0
    assert selected_geo_info[2] == {'density': 75, 'high': True}

    person_select = test_utils.get_table_records(db_config, 'person', ['id', 'name', 'city_id'])
    logging.info(f'Person records: {person_select}')
    assert len(person_select) == 2
    assert person_select[0][0] == 1
    assert person_select[0][1] == 'adam'
    assert person_select[0][2] == 123
    assert person_select[1][0] == 2
    assert person_select[1][1] == 'craig'
    assert person_select[1][2] == 24

    city.capital = True
    geo_info.area = 34
    citizens[0].name = 'bob'
    py2sql.save_object(city)

    geo_info_select = test_utils.get_table_records(db_config, 'geo_info', ['id', 'area', 'tags'])
    logging.info(f'Geo info records: {geo_info_select}')
    assert len(geo_info_select) == 1
    assert geo_info_select[0][1] == 34.0

    city_select = test_utils.get_table_records(db_config, 'city', ['id', 'name', 'capital', 'geo_info_id', 'geo_info_new_id'])
    logging.info(f'City records: {city_select}')
    assert len(city_select) == 1
    assert city_select[0][2] == True

    person_select = test_utils.get_table_records(db_config, 'person', ['id', 'name', 'city_id'])
    logging.info(f'Person records: {person_select}')
    assert len(person_select) == 2
    assert person_select[0][1] == 'bob'

    py2sql.delete_hierarchy(City)

    db_tables = py2sql.db_tables
    logging.info(f'Database tables: {db_tables}')
    assert db_tables == ['person']

    py2sql.db_disconnect()

    test_utils.drop_all_tables(db_config)
