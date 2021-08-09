import Main as m

# all_institution_data()
# all_moveon_data()
''' 
# TODO completar argumentos
surname = 'valentina'
first_name = 'amariles'
gender = '2'
entity = 'person'
opcional = {}
create_person(entity,surname,first_name,gender,opcional)

entity = 'person'
person_id = create_person(entity, "daniel prueba", "sanchez prueba", 2, {})
actualizar = {
    'entity': 'person',
    'person.id': person_id,
    'person.first_name': "fabian",
    'person.surname': "ribon"
}
print(str(actualizar))
print(update_person(entity, actualizar))


name = 'david daniel prueba alejandro'
person_id = '647'
Direction = '2'
entity = 'stay'
opcional = {}
create_stay(entity, name, person_id, Direction, opcional)


entity = 'stay'
stay_id = create_stay(entity, "david daniel", 15071, 3, {})
actualizar = {
    'entity': entity,
    'stay.id': stay_id,
    'stay.person_id': '15071',
    'stay.name': 'akcel david',
    'stay.direction': '1'
}
print(str(actualizar))
print(update_stay(entity, actualizar))
'''
'''
first_name = 'barreto'
surname = 'samuel'
institution_id = '120'
entity = 'contact'
opcional = {}
create_contact(entity,first_name,surname,institution_id, opcional)

entity = 'contact'
contact_id = create_contact(entity, "Daniel Prueba", "Akcel Prueba", 290, 2, {})
actualizar = {
    'entity' : entity,
    'contact.id': contact_id,
    'contact.first_name': "Daniel funciono la actualizacion"
}
print(str(actualizar))
print(update_contact(entity, actualizar))

name = 'relation'
status = 'terminated'
relation_type_id = '1'
entity = 'relation'
opcional = {}
create_relation(entity,name,status,relation_type_id, opcional)

entity = 'relation'
relation_id =create_relation(entity, "relation",2, 1, {})
actualizar = {
    'entity':entity ,
    'relation.id' : relation_id,
    'relation.name': 'daniel',


}
print(str(actualizar))
print(update_relation(entity, actualizar))



institution_id = '43',
role_id= '2',
relation_id = '2453'
entity = 'relation-institution'
opcional = {}
create_relation_institution(entity,institution_id,role_id,relation_id, opcional)


entity = 'relation-institution'
relation_institution_id = m.create_relation_institution(entity,114,2,3010, {})
actualizar = {
    'entity': entity,
    'relation_institution.id': 11374,
    'relation_institution.role.id': 3
}
print(str(actualizar))
print(m.update_relation_institution(entity, actualizar))
'''
'''
contact_institution = '2',
contac_id= '3718',
entity = 'relation-contact'
opcional = {}
create_relation_contact(entity,contact_institution,contac_id, opcional)


entity = 'relation-contact'
relation_contact_id =create_relation_contact(entity, 2,3718, {})
actualizar = {
    'entity' :entity,
    'relation_contact.id' : relation_contact_id,
    'relation_contact.contact.id':'3718'

}
print(str(actualizar))
print(update_relation_contact(entity, actualizar))

'''