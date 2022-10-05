from glob import glob
import json 
import os
import random
import itertools
from itertools import cycle

offshore_list= []
onshore_list = []
rooftop_list = []
openfield_list = []
sequence = '/storage/internal/data/nestor/input_data/renewables/*/*'
blob = glob(sequence)
current_used_onshore_cases = ['Paper_wind_1' ,'Paper_wind_2' ,'Paper_wind_2_landscapes' ,'Paper_wind_3' ,'Paper_wind_4' , 'S2_Expansive_noForest_IA1000_OA800_scenicness_threshold6', 'S2_Expansive_IA800_OA600']

for path in blob:
    basename = os.path.basename(path)
    
    if basename.startswith('Paper_Offshore'):
        offshore_list.append(basename)
    elif basename in current_used_onshore_cases :
        onshore_list.append(basename)
    elif basename.startswith('rooftop'):
        rooftop_list.append(basename)
    elif basename.startswith('paper_SQR30'):
        openfield_list.append(basename)

json_path = '/storage/internal/home/d-nuga/nestor/nestor/data/scenario_definition/newTHG0_withoutEE.json'




a = [offshore_list,onshore_list,rooftop_list,openfield_list]
combinations =  list(itertools.product(*a))
print(onshore_list, '\n', combinations,'\n',len(combinations))
# value = random.choice(combinations)
value = combinations[14]
print(value)


with open(json_path, 'r') as file:
    content = json.load(file)
    
    # print(content['renewables']['rooftop_pv']['nestor_ee_case'])
    content['renewables']['offshore']['nestor_ee_case'] = value[0]
    content['renewables']['onshore']['nestor_ee_case'] = value[1]
    print('cases considered: ', content['renewables']['offshore']['nestor_ee_case'], content['renewables']['onshore']['nestor_ee_case'] )
    
with open(json_path, "w") as file:
    json.dump(content, file, indent=4)

print('New cases generated')