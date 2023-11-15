import csv, os
from combination_gen import gen_comb_list

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
    
    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
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

    def pivot_table(self, keys_to_pivot_list, keys_to_aggregate_list, aggregate_func_list):
        # First create a list of unique values for each key
        unique_values_list = []
        for key in keys_to_pivot_list:
            unique_values = list(set(item[key] for item in self.table))
            unique_values_list.append(unique_values)

        # Get the combination of unique values list using the gen_comb_list function
        combinations = gen_comb_list(unique_values_list)

        # Initialize the pivot table
        pivot_table = Table(self.table_name + '_pivot', [])

        # Iterate through each combination
        for combo in combinations:
            # Filter the table based on the combination
            filtered_table = self
            for idx in range(len(keys_to_pivot_list)):
                key = keys_to_pivot_list[idx]
                value = combo[idx]
                filtered_table = filtered_table.filter(lambda x, k=key, v=value: x[k] == v)

            # Apply aggregate functions to keys to aggregate
            aggregated_values = []
            for idx in range(len(keys_to_aggregate_list)):
                key = keys_to_aggregate_list[idx]
                aggregate_function = aggregate_func_list[idx]
                aggregated_value = filtered_table.aggregate(aggregate_function, key)
                aggregated_values.append(aggregated_value)

            # Combine the pivot keys and aggregated values into a list
            pivot_row = [combo] + [aggregated_values]

            # Append the row to the pivot table
            pivot_table.table.append(pivot_row)

        return pivot_table

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
print(f"average games ranking below 10 vs ranking above 10 {avg_below} vs {avg_above}")
print(f"middle fielder passes vs forward passes {mid_pass} vs {forward_pass}")

first_class = table2.filter(lambda x: x['class'] == '1')
first_class_paid = first_class.aggregate(lambda x: sum(x)/len(x), 'fare')

third_class = table2.filter(lambda x: x['class'] == '3')
third_class_paid = third_class.aggregate(lambda x: sum(x)/len(x), 'fare')

survival_male = table2.filter(lambda x: x['gender'] == 'M' and x['survived'] == 'yes')
survived_count_male = len(survival_male.table)

survival_female = table2.filter(lambda x: x['gender'] == 'F' and x['survived'] == 'yes')
survived_count_female = len(survival_female.table)

survived_male_southampton = survival_male.filter(lambda x: x['embarked'] == 'Southampton')
survived_male_southampton_count = len(survived_male_southampton.table)

print(f"survival male and female is {survived_count_male} vs {survived_count_female}")
print(f"male passenger embraked at southampton is {survived_male_southampton_count}")
table4 = Table('titanic', Titanic)
my_DB.insert(table4)
my_table4 = my_DB.search('titanic')
my_pivot = my_table4.pivot_table(['embarked', 'gender', 'class'], ['fare', 'fare', 'fare', 'last'], [lambda x: min(x), lambda x: max(x), lambda x: sum(x)/len(x), lambda x: len(x)])
print(my_pivot)

