import json
import os.path
import queue_handelert
import xml.etree.ElementTree as ET
import pandas as pd



def all_institution_data():
    entity = 'institution'
    data = '{"filters":"{\\"groupOp\\":\\"AND\\",\\"rules\\":[{\\"field\\":\\"institution.name\\",\\"op\\":\\"cn\\",' \
           '\\"data\\":\\"\\"}]}","visibleColumns":"institution.id;institution.name","locale":"eng",' \
           '"sidx":"institution.name","sord":"asc","sortName":"institution.name","sortOrder":"asc","_search":"true",' \
           '"page":"1","rows":"250"}'

    response = queue_handelert.list_request(entity, data)

    XML_response = ET.fromstring(response)

    pages = int(XML_response[0][0][0][2].text)
    keys = XML_response[0][0][0][3].text
    columns = keys.split(';')
    table = {}
    for key in columns:
        table[key] = []
    for page in range(pages):
        page_req = page + 1
        new_data = '{"filters":"{\\"groupOp\\":\\"AND\\",\\"rules\\":[{\\"field\\":\\"institution.name\\",\\"op\\":\\"cn\\",\\"data\\":\\"\\"}]}","visibleColumns":"' + keys + '","locale":"eng","sidx":"institution.name","sord":"asc","sortName":"institution.name","sortOrder":"asc","_search":"true","page":"' + str(
            page_req) + '","rows":"250"} '
        new_response = queue_handelert.list_request(entity, new_data)
        new_XML_response = ET.fromstring(new_response)
        xml_data = new_XML_response[0][0][0]
        for row in xml_data.findall('rows'):
            for key in columns:
                array = table[key]
                array.append(row.find(key).text)
                table[key] = array
    df = pd.DataFrame(table)
    df.to_excel('Institution.xlsx', index=False)
    return df


def all_moveon_data():
    entities = ["stay","contact","relation-institution", "relation-contact", "relation-content-type", "institution", "person",
                 "relation"]
    for entity in entities:
        original_entity = entity
        if entity.__contains__('-'):
            entity = entity.replace('-', '_')
        result = test_request(entity, original_entity)
        pages = result["pages"]
        keys = result["keys"]
        if keys.__contains__("person.address.country.iso2"):
            keys = keys.replace("person.address.country.iso2;", "")
        if keys.__contains__("person.address.country.iso3"):
            keys = keys.replace("person.address.country.iso3;", "")
        columns = keys.split(';')
        table = {}
        for key in columns:
            table[key] = []
        for page in range(pages):
            page_req = page + 1
            new_data = '{"filters":"{\\"groupOp\\":\\"AND\\",\\"rules\\":[{\\"field\\":\\"' + entity + '.id\\",\\"op\\":\\"cn\\",\\"data\\":\\"\\"}]}","visibleColumns":"' + keys + '","locale":"eng","sidx":"' + entity + '.id","sord":"asc","sortName":"' + entity + '.id","sortOrder":"asc","_search":"true","page":"' + str(
                page_req) + '","rows":"250"} '
            new_response = queue_handelert.list_request(original_entity, new_data)
            new_XML_response = ET.fromstring(new_response)
            xml_data = new_XML_response[0][0][0]
            for row in xml_data.findall('rows'):
                for key in columns:
                    array = table[key]
                    array.append(row.find(key).text)
                    table[key] = array
        df = pd.DataFrame(table)
        df.to_excel(entity + '.xlsx', index=False)


def test_request(entity, original_entity):
    data = '{"filters":"{\\"groupOp\\":\\"AND\\",\\"rules\\":[{\\"field\\":\\"' + entity + '.id\\",\\"op\\":\\"cn\\",\\"data\\":\\"\\"}]}","visibleColumns":"' + entity + '.id","locale":"eng","sidx":"' + entity + '.id","sord":"asc","sortName":"' + entity + '.id","sortOrder":"asc","_search":"true","page":"1","rows":"250"} '
    response = queue_handelert.list_request(original_entity, data)
    XML_response = ET.fromstring(response)
    xml_data = XML_response[0][0][0]
    pages = int(xml_data.find("total").text)
    keys = xml_data.find("availableKeys").text
    result = {"pages": pages, "keys": keys}
    return result


def create_person(entity, surname, first_name, gender, opcional):
    todos = {
        'entity': entity,
        'person.surname': surname,
        'person.first_name': first_name,
        'person.gender.id': gender
    }
    todos.update(opcional)
    data = json.dumps(todos)
    print("data: ")
    print(data)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    p_id = ""
    for result in XML_response.iter('id'):
        p_id = result.text
    return p_id


def update_person(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def create_stay(entity, name, person_id, direction, opcional):
    todos = {
        'entity': entity,
        'stay.name': name,
        'stay.person_id': person_id,
        'stay.direction_id': direction
    }
    todos.update(opcional)
    data = json.dumps(todos)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    s_id = ""
    for result in XML_response.iter('id'):
        s_id = result.text
    return s_id


def update_stay(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def create_contact(entity, first_name, surname, institution_id, opcional):
    todos = {
        'entity': entity,
        'contact.first_name': first_name,
        'contact.surname': surname,
        'contact.institution_id': institution_id

    }
    todos.update(opcional)
    data = json.dumps(todos)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    p_id = ""
    for result in XML_response.iter('id'):
        p_id = result.text
    return p_id


def update_contact(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def create_relation(entity, name, status, relation_type_id, opcional):
    todos = {
        'entity': entity,
        'relation.name': name,
        'relation.status.id': status,
        'relation.relation_type.id': relation_type_id

    }
    todos.update(opcional)
    data = json.dumps(todos)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    p_id = ""
    for result in XML_response.iter('id'):
        p_id = result.text
    return p_id


def update_relation(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def create_relation_institution(entity,relation_insti_id ,institution_id, role_id, relation_id, opcional):
    todos = {
        'entity': entity,
        'relation_institution.id' : relation_insti_id,
        'relation_institution.institution.id': institution_id,
        'relation_institution.role.id': role_id,
        'relation_institution.relation.id': relation_id

    }
    todos.update(opcional)
    data = json.dumps(todos)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    p_id = ""
    for result in XML_response.iter('id'):
        p_id = result.text
    return p_id


def update_relation_institution(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def create_relation_contact(entity, contact_institution, contac_id, opcional):
    todos = {
        'entity': entity,
        'relation_contact.institution': contact_institution,
        'relation_contact.contact.id': contac_id

    }
    todos.update(opcional)
    data = json.dumps(todos)
    response = queue_handelert.create_request(entity, data)
    XML_response = ET.fromstring(response)
    p_id = ""
    for result in XML_response.iter('id'):
        p_id = result.text
    return p_id


def update_relation_contact(entity, body):
    data = json.dumps(body)
    response = queue_handelert.update_request(entity, data)
    return response


def read_file(file_path):
    home = os.getcwd()
    cert_path = os.path.join(home, file_path)
    df = pd.read_excel(cert_path)
    return df  # tiene que regresar un data frame con toda la informacion del archivo de excel


# TODO traer la tabla de MoveON de la entidad pedida y guardarla en un DF
def request_table_from_moveon(entity):
    original_entity = entity
    if entity.__contains__('-'):
        entity = entity.replace('-', '_')
    result = test_request(entity, original_entity)
    pages = result["pages"]
    keys = result["keys"]
    if keys.__contains__("person.address.country.iso2"):
        keys = keys.replace("person.address.country.iso2;", "")
    if keys.__contains__("person.address.country.iso3"):
        keys = keys.replace("person.address.country.iso3;", "")
    columns = keys.split(';')
    table = {}
    for key in columns:
        table[key] = []
    for page in range(pages):
        page_req = page + 1
        new_data = '{"filters":"{\\"groupOp\\":\\"AND\\",\\"rules\\":[{\\"field\\":\\"' + entity + '.id\\",\\"op\\":\\"cn\\",\\"data\\":\\"\\"}]}","visibleColumns":"' + keys + '","locale":"eng","sidx":"' + entity + '.id","sord":"asc","sortName":"' + entity + '.id","sortOrder":"asc","_search":"true","page":"' + str(
            page_req) + '","rows":"250"} '
        new_response = queue_handelert.list_request(original_entity, new_data)
        new_XML_response = ET.fromstring(new_response)
        xml_data = new_XML_response[0][0][0]
        for row in xml_data.findall('rows'):
            for key in columns:
                array = table[key]
                array.append(row.find(key).text)
                table[key] = array
    df = pd.DataFrame(table)
    return df  # tiene que regresar un data frame con toda la informacion de la entidad de MoveOn

def edicion_datos_stay(row, entity,df_frameworks,df_stayopt):
    if entity == 'stay' :
        nombre_framework = row['stay.framework']
        for index, row_f in df_frameworks.iterrows():
            if row_f['Name'] == nombre_framework :
                row['stay.framework'] = row_f['Framework: ID']

        stay_opt = row['stay.id']
        for index, row_opt in df_stayopt.iterrows():
            if row_opt['Stay: ID'] == stay_opt and row_opt['Status selection'] == 'Selected':
                row['stay.stay_opportunity'] = row_opt['Relation: ID']

        if row['stay.status'] == 'New registration' :
            row['stay.status'] = 1

        elif row['stay.status'] == 'Completed':
            row['stay.status'] = 2

        elif row['stay.status'] == 'Current':
            row['stay.status'] = 3

        elif row['stay.status'] == 'Interrupted':
            row['stay.status'] = 4

        elif row['stay.status'] == 'Completed':
            row['stay.status'] = 5

        elif row['stay.status'] == 'Not accepted':
            row['stay.status'] = 6

        elif row['stay.status'] == 'Planned':
            row['stay.status'] = 7

        return row
    else :
        return row

# TODO metodo que compara el xlsx con la info de MoveON y crea 2 dataframe, uno con la info a crear y otro con la info a actualizar
def compare_file_to_moveon(entity, file_path):
    df_moveon = request_table_from_moveon(entity)
    df_xlsx = read_file(file_path)
    df_frameworks = read_file('Frameworks.xlsx')
    df_stay_opt = read_file('AcademicMoveWishesOutgoing.xlsx')
    #print(df_moveon)
    #print(df_xlsx)
    # TODO comparar ambos dataframes, y crear los 2 nuevos
    columns = df_xlsx.columns
    df_create = pd.DataFrame(columns=columns)
    df_update = pd.DataFrame(columns=columns)
    for index, row in df_xlsx.iterrows():
        copiaentity = entity
        if entity.__contains__('-'):
            copiaentity = copiaentity.replace('-', '_')
        entity_id = row[copiaentity + '.id']
        if str(entity_id) == 'nan' :
            #print("Crear: ")
            #print(row)
            teprow = edicion_datos_stay(row,entity,df_frameworks,df_stay_opt)
            df_create = df_create.append(teprow)
        else:
            for index_2, row_2 in df_moveon.iterrows():
                entity_id_2 = row_2[copiaentity + '.id']
                if str(entity_id) == str(entity_id_2):
                    different = False
                    for column in columns:
                        if row[column] != row_2[column]:
                            different = True
                    if different is True:
                        print("Actualizar: ")
                        teprow = edicion_datos_stay(row, entity,df_frameworks,df_stay_opt)
                        print(row)
                        df_update = df_update.append(teprow)
    #print(df_create)
    #print(df_update)
    return df_create, df_update



def carga_masiva(entity, filepath):
    df_create, df_update = compare_file_to_moveon(entity, filepath)

    for index, row in df_create.iterrows():
        if entity == "person":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "person.surname" or data == "person.first_name" or data == "person.gender.id":
                    pass
                else:
                    opcional[data] = row[data]
            #print("json query:")
            #print("surname" + row["person.surname"])
            #print(opcional)
            create_person(entity, row["person.surname"], row["person.first_name"], row["person.gender.id"], opcional)
        if entity == "stay":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "stay.name" or data == "stay.person_id" or data == "stay.direction_id" or data == "stay.academic_period_start" or data == "stay.status_fra" \
                        or data == "stay.status_deu" or data == "stay.status_ita" or data == "stay.status_spa" or data == "stay.stay_type" or data == "stay.academic_period_end" or data == "stay.status" or data == "stay.academic_period_start" or data == "stay.framework_id":
                    pass
                else:
                    opcional[data] = row[data]
            #print("json query:")
            #print("id " + row["stay.person_id"])
            #print(opcional)
            create_stay(entity, row["stay.name"], row["stay.person_id"], row["stay.direction_id"], opcional)
        if entity == "contact":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "contact.first_name" or data == "contact.surname"  or data == "contact.institution_id":
                    pass
                else:
                    opcional[data] = row[data]
            create_contact(entity, row["contact.first_name"], row["contact.surname"],row["contact.institution_id"], opcional)
        if entity == "relation":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "relation.name" or data == "relation.status.id" or data == "relation.relation_type.id" :
                    pass
                else:
                    opcional[data] = row[data]
            create_relation(entity, row["relation.name"], row["relation.status.id"], row["relation.relation_type.id"], opcional)
        if entity == "relation-institution":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data =="relation_institution.id" or data == "relation_institution.institution.id" or data == "relation_institution.role.id" or data == "relation_institution.relation.id":
                    pass
                else:
                    opcional[data] = row[data]
            create_relation_institution(entity, row ["relation_institution.id"], row["relation_institution.institution.id"], row["relation_institution.role.id"],row["relation_institution.relation.id"],opcional)
        if entity == "relation-contact":
            opcional = {}
            columns = df_create.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "relation_contact.institution" or data == "relation_contact.contact.id" :
                    pass
                else:
                    opcional[data] = row[data]
            create_relation_contact(entity, row["relation_contact.institution"],row["relation_contact.contact.id"], opcional)


    for index, row in df_update.iterrows():
        if entity == "person":
            opcional = {
               'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan":
                    pass
                else:
                    opcional[data] = row[data]
            print("json query:")
            print(opcional)
            update_person(entity, opcional)
        if entity == "stay":
            opcional = {
                'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan" or data == "stay.academic_period_start" or data == "stay.status_fra" \
                    or data == "stay.status_deu" or data == "stay.status_ita" or data == "stay.status_spa" or data == "stay.stay_type" or data == "stay.academic_period_end" or data == "stay.status" or data == "stay.framework_id":
                    pass
                else:
                    opcional[data] = row[data]
            print("json query:")
            print(opcional)
            update_stay(entity, opcional)
        if entity == "contact":
            opcional = {
                'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan":
                    pass
                else:
                    opcional[data] = row[data]
            update_contact(entity, opcional)
        if entity == "relation":
            opcional = {
                'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan":
                    pass
                else:
                    opcional[data] = row[data]
            update_relation(entity, opcional)
        if entity == "relation-institution":
            opcional = {
                'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan":
                    pass
                else:
                    opcional[data] = row[data]
            update_relation_institution(entity, opcional)
        if entity == "relation-contact":
            opcional = {
                'entity': entity
            }
            columns = df_update.columns
            for data in columns:
                if str(row[data]) == "nan":
                    pass
                else:
                    opcional[data] = row[data]
            update_relation_contact(entity, opcional)

    # TODO llamar metodo de crear de la entidad correspondiente para cada una de las filas del dataframe
    # TODO llamar metodo de actualizar de la entidad correspondiente para cada una de las filas del dataframe

carga_masiva("stay","stay - copia.xlsx")

'''
carga_masiva("relation", "relation.xlsx")#(listo)

carga_masiva("contact", "contact.xlsx")#(listo)
carga_masiva("relation-institution", "relation_institution.xlsx")#(listo)
carga_masiva("person", "person.xlsx")#(listo)

all_moveon_data()

carga_masiva("relation-contact", "relation_contact.xlsx")
ok

'''