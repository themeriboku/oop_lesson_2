import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

Players = []
with open(os.path.join(__location__, 'Players.csv.')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        Players.append(dict(r))

Titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        Titanic.append(dict(r))

Teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        Teams.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('Players', Players)
table2 = Table('Titanic', Titanic)
table3 = Table('Teams', Teams)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)


b = table1.filter(lambda x: 'ia' in x['team'])
c = b.filter(lambda x: int(x['minutes']) < 200)
p = c.filter(lambda x: int(x['passes']) > 100)
selected_players = p.select(['surname', 'team', 'position'])

ranking_below = table3.filter(lambda x: int(x['ranking']) < 10)
avg_below = ranking_below.aggregate(lambda x: sum(x)/len(x), 'games')

ranking_above = table3.filter(lambda x: int(x['ranking']) >= 10)
avg_above = ranking_above.aggregate(lambda x: sum(x)/len(x), 'games')

mid_fields = table1.filter(lambda x: 'midfielder' in x['position'])
mid_pass = mid_fields.aggregate(lambda x: sum(x)/len(x), 'passes')
forward = table1.filter(lambda x: 'forward' in x['position'])
forward_pass = forward.aggregate(lambda x: sum(x)/len(x), 'passes')

print(selected_players)
print(f"average games ranking below 10 vs ranking above 10{avg_below} vs {avg_above}")
print(f"middle fielder passes vs forward passes{mid_pass} vs {forward_pass}")

first_class = table2.filter(lambda x: x['class'] == '1')
first_class_paid = first_class.aggregate(lambda x: sum(x)/len(x), 'fare')

third_class = table2.filter(lambda x: x['class'] == '3')
third_class_paid = third_class.aggregate(lambda x: sum(x)/len(x), 'fare')

survival_male = table2.filter(lambda x: x['gender'] == 'M' and x['survived'] == 'yes')
survived_count_male = len(survival_male.table)

survival_female = table2.filter(lambda x: x['gender'] == 'F' and x['survived'] == 'yes')
survived_count_female = len(survival_female.table)

print(f"survival male and female is {survived_count_male} vs {survived_count_female}")











