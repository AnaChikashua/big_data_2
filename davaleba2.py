from typing import List, Set

from neo4j import GraphDatabase


class myGraphApp:
    """ კლასი გრაფთან მუშაობისთვის. """

    def __init__(self, url="neo4j://localhost:7687", user="neo4j", pasw="test123"):
        """ კლასის კონსტრუქტორი """
        # ატრიბუტი ბაზასთან დაკავშირებისთვის
        self.conn = GraphDatabase.driver(url, auth=(user, pasw))

    def close(self):
        """ ფუნქცია წყვეტს კავშირს ბაზასთან. """
        self.conn.close()

    def create_session(self):
        """ ფუნქცია ხსნის სესიას (კავშირის არხი) ბაზისთვის ბრძანების გადაწოდების მიზნით. """
        # თუ ბაზასთან დაკავშირება არ არსებობს, მაშინ კავშირის არხის გახსნა იქნება შეუძლებელი.
        assert self.conn is not None, "connection not initialized"
        return self.conn.session()  # გავხსნათ კავშირის არხი და დავაბრუნოთ შესაბამისი ობიექტი

    def add_record(self, label, name):
        """ ფუნქცია აგენერირებს და აბრუნებს ერთ ველიანი ობიექტის დამატებისთვის შესაბამის ბრძანებას.
            label პარამეტრი განსაზღვრავს ობიექტის ტიპს, მაგალითად, LastName, FirtsName, Subject და ა.შ.
            name პარამეტრი განსაზღვრავს ობიექტის სახელს. """
        return f"create (f:{label} {{name: '{name}'}})"

    def create_relation(self, obj1_label, obj1_name, obj2_label, obj2_name, rel_text):
        """ ფუნქცია აგენერირებს და აბრუნებს ორ ერთ ველიან (განსხვავებული ან ერთიდაიმავე ტიპის) ობიექტს
            შორის კავშირის დამატებისთვის შესაბამის ბრძანებას. გავშირი იქმნება პირველი ობიექტიდან მეორეს მიმართულებით.
            ატრიბუტი label დანამატით განსაზღვრავს ობიექტის ტიპს
            ატრიბუტი name დანამატით განსაზღვრავს ობიექტის სახელს.
            rel_text ატრიბუტი განსაზღვრავს კავშირის ტიპის ტექსტურ ჩანაწერს, მაგალითად, LastName და FirtsName
                ობიექტებს შორის შეიძლება ჩაიწეროს ტექსტი: "object_name" (თქვენი შეხედულებით) """
        return f"""
        MATCH (a:{obj1_label}), (b:{obj2_label})
        WHERE a.name = '{obj1_name}' AND b.name = '{obj2_name}'
        CREATE (a)-[r:{rel_text} {{name: a.name + '<->' + b.name}}]->(b)
        """

    def remove_record(self, label=None, name=None):
        """ ფუნქცია აგენერირებს და აბრუნებს ერთ კონკრეტურლი ობიექტის წაშლის, ან კონკრეტული ტიპის ყველა ჩანაწერის
            წაშლის, ან მთლიანად ბაზის გასუფთავების შესაბამის ბრძანებას.
            თუ name პარამეტრი მითითებულია, მაშინ აუცილებელია მითითებული იყოს label პარამეტრიც. ამ შემთხვევაში
                წაიშლება კონკრეტული ობიექტი.
            თუ name პარამეტრი მითითებული არაა, მაგრამ მითითებულია label პარამეტრი, მაშინ წაიშლება კონკრეტული
                ტიპის მქონე ყველა ობიექტი
            თუ არც name და არც label პარამეტრი არაა მითითებული, მაშინ გასუფთავდება ბაზა.  """
        s = "match (n"
        if label:
            s += f":{label}"
            if name:
                s += f" {{name: '{name}' }}"
        s += ") detach delete n"
        return s


def get_data(fname):
    with open(fname, 'r') as read:
        data = read.readlines()
    cleared_data = list()
    for i, line in enumerate(data):
        cleared_data.append(line[:-1].split('#'))
    return cleared_data


def get_unique(objects):
    unique_fname = set()
    unique_lname = set()
    unique_subjects = set()
    for object in objects:
        unique_fname.add(object['FirstName'])
        unique_lname.add(object['LastName'])
        for _subject in object['Subject']:
            unique_subjects.add(_subject)
    return list(unique_fname), list(unique_lname), list(unique_subjects)


if __name__ == '__main__':
    fname = 'students.in'
    myObj = myGraphApp()
    data = get_data(fname)
    objects = []
    for student in data:
        obj = {
            'LastName': student[0],
            'FirstName': student[1],
            'Subject': []
        }
        for i in range(len(student[2:-1:2])):
            obj['Subject'].append(student[2 * (i + 1)])
        objects.append(obj)
    unique_data = get_unique(objects)
    with myObj.create_session() as session:
        queries = []
        relations = []
        for data in unique_data[0]:
            queries.append(myObj.add_record("LastName", data))
        for data in unique_data[1]:
            queries.append(myObj.add_record("FirstName", data))
        for data in unique_data[2]:
            queries.append(myObj.add_record('Subject', data))
        for data in objects:
            relations.append(
                myObj.create_relation('LastName', data['LastName'], 'FirstName', data['FirstName'], 'StudentName'))
            for subject in data['Subject']:
                query = myObj.create_relation('FirstName', data['FirstName'], 'Subject', subject, 'Passed')
                relations.append(query)

        for query in queries:
            session.run(query)

        for relation in relations:
            session.run(relation)

    myObj.close()
